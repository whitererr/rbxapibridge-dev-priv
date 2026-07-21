local Bridge = {}
Bridge.__index = Bridge

local MAX_RECONNECT_ATTEMPTS = 5
local INITIAL_RECONNECT_DELAY = 1
local MAX_RECONNECT_DELAY = 15
local STABLE_CONNECTION_SECONDS = 10
local HANDLER_PROTOCOL_VERSION = 1

local function retry_delay(attempt)
    return math.min(INITIAL_RECONNECT_DELAY * (2 ^ (attempt - 1)), MAX_RECONNECT_DELAY)
end

local function table_count(values)
    local count = 0
    for _ in pairs(values) do count += 1 end
    return count
end

local function is_permanent_connect_error(message)
    local text = string.lower(tostring(message or ""))
    return string.find(text, "unauthorized", 1, true) ~= nil
        or string.find(text, "authentication", 1, true) ~= nil
        or string.find(text, "401", 1, true) ~= nil
        or string.find(text, "403", 1, true) ~= nil
        or string.find(text, "invalid url", 1, true) ~= nil
end

function Bridge.new()
    if shared.__active_bridge and type(shared.__active_bridge.Disconnect) == "function" then
        pcall(shared.__active_bridge.Disconnect, shared.__active_bridge)
        print("[Bridge] Stopped previous bridge instance.")
    end

    local self = setmetatable({}, Bridge)
    self.handlers = {}
    self.synced_handlers = {}
    self.handler_revision = 0
    self.handler_sync_ready = false
    self.handler_sync_failed_revision = 0
    self.uuid = nil
    self.url = nil
    self.ws = nil
    self.connected = false
    self.running = false
    self.connection_state = "Disconnected"
    self.connection_generation = 0
    self.retry_attempt = 0
    self.permanent_sync_error = nil
    self.update_failures = {}

    while not game:IsLoaded() do task.wait(1) end
    local player = game.Players.LocalPlayer
    while not player do task.wait(1); player = game.Players.LocalPlayer end
    self.client_id = tostring(player.UserId)

    shared.__active_bridge = self
    return self
end

function Bridge:On(handler_name, callback)
    self.handlers[handler_name] = callback
end

function Bridge:_compile_handler(entry)
    if entry.server_owned then
        return true, nil
    end
    if type(entry.id) ~= "string" or entry.id == "" or type(entry.source) ~= "string" then
        return false, "handler_source_invalid: invalid handler entry"
    end
    if type(loadstring) ~= "function" then
        return false, "client_dynamic_handlers_unsupported: loadstring is unavailable"
    end
    local compile_ok, compiled, compile_error = pcall(loadstring, entry.source, "@bridge-handler/" .. entry.id)
    if not compile_ok then
        return false, "handler_compile_failed: " .. tostring(compiled)
    end
    if not compiled then
        return false, "handler_compile_failed: " .. tostring(compile_error)
    end
    local loaded, exported = pcall(compiled)
    if not loaded then
        return false, "handler_load_failed: " .. tostring(exported)
    end
    if type(exported) == "function" then
        return true, exported
    elseif type(exported) == "table" and type(exported.Run) == "function" then
        return true, function(args) return exported.Run(args) end
    elseif exported ~= nil then
        return true, function() return exported end
    end
    return false, "handler_source_invalid: handler must return a function, a table with Run, or a result"
end

function Bridge:_send_payload(payload, purpose)
    local ws = self.ws
    if not ws or not self.connected then
        warn("[Bridge] Cannot send " .. tostring(purpose) .. ": WebSocket is not connected")
        return false
    end
    local encoded_ok, encoded_or_error = pcall(game.HttpService.JSONEncode, game.HttpService, payload)
    if not encoded_ok then
        warn("[Bridge] Cannot encode " .. tostring(purpose) .. ": " .. tostring(encoded_or_error))
        return false
    end
    task.spawn(function()
        if self.ws ~= ws or not self.connected then
            return
        end
        local sent, send_result, send_error = pcall(function()
            ws:Send(encoded_or_error)
        end)
        if not sent or send_result == false then
            warn("[Bridge] Failed to send " .. tostring(purpose) .. ": " .. tostring(send_error or send_result))
        end
    end)
    return true
end

function Bridge:_send_sync_result(revision, success, handler_errors, message)
    local payload = {
        type = "handler_sync_result",
        protocol = HANDLER_PROTOCOL_VERSION,
        revision = revision,
        success = success,
        loaded_handlers = table_count(self.synced_handlers),
        handler_errors = handler_errors,
        error = message
    }
    return self:_send_payload(payload, "handler synchronization acknowledgement")
end

function Bridge:_apply_handler_manifest(data)
    if data.protocol ~= HANDLER_PROTOCOL_VERSION or type(data.revision) ~= "number" or data.revision < 1 or type(data.handlers) ~= "table" then
        self.handler_sync_ready = false
        self.permanent_sync_error = "protocol_mismatch: unsupported handler manifest"
        self:_send_sync_result(data.revision or 0, false, nil, "protocol_mismatch: unsupported handler manifest")
        return
    end
    local next_registry = {}
    local failures = {}
    local seen = {}
    for _, entry in ipairs(data.handlers) do
        local id = type(entry) == "table" and entry.id or nil
        if type(id) ~= "string" or id == "" then
            failures[tostring(id or "unknown")] = "handler_source_invalid: invalid handler identifier"
        elseif seen[id] then
            failures[id] = "duplicate_handler_identifier: " .. id
        else
            seen[id] = true
            local ok, handler_or_error = self:_compile_handler(entry)
            if not ok then
                failures[id] = handler_or_error
            elseif handler_or_error ~= nil then
                next_registry[id] = handler_or_error
            end
        end
    end
    self.synced_handlers = next_registry
    self.handler_revision = data.revision
    self.handler_sync_ready = true
    self.handler_sync_failed_revision = next(failures) and data.revision or 0
    self.update_failures = {}
    self.permanent_sync_error = nil
    print("[Bridge] Handler manifest applied: revision " .. tostring(data.revision) .. ", loaded " .. tostring(table_count(next_registry)))
    self:_send_sync_result(data.revision, next(failures) == nil, next(failures) and failures or nil,
        next(failures) and "handler_manifest_partially_loaded" or nil)
end

function Bridge:_apply_handler_update(data)
    self.handler_sync_ready = false
    if data.protocol ~= HANDLER_PROTOCOL_VERSION or type(data.revision) ~= "number" or data.revision < self.handler_revision or type(data.handler_entry) ~= "table" then
        self.handler_sync_ready = false
        self.handler_sync_failed_revision = data.revision or 0
        self.permanent_sync_error = "protocol_mismatch: invalid handler update"
        self:_send_sync_result(data.revision or 0, false, nil, "protocol_mismatch: invalid handler update")
        return
    end
    local entry = data.handler_entry
    if self.handler_sync_failed_revision ~= data.revision then
        self.handler_sync_failed_revision = 0
        self.update_failures = {}
    end
    local ok, handler_or_error = self:_compile_handler(entry)
    if not ok then
        self.synced_handlers[tostring(entry.id or "unknown")] = nil
        self.handler_sync_failed_revision = data.revision
        self.handler_revision = data.revision
        self.update_failures[tostring(entry.id or "unknown")] = handler_or_error
        if data.final then
            self.handler_sync_ready = true
            self:_send_sync_result(data.revision, false, self.update_failures, "handler_update_partially_loaded")
        end
        return
    end
    self.synced_handlers[entry.id] = handler_or_error
    self.update_failures[entry.id] = nil
    self.handler_revision = data.revision
    if data.final then
        self.handler_sync_ready = true
        local partial = next(self.update_failures) ~= nil
        self:_send_sync_result(data.revision, not partial, partial and self.update_failures or nil,
            partial and "handler_update_partially_loaded" or nil)
    end
end

function Bridge:_apply_handler_remove(data)
    self.handler_sync_ready = false
    if data.protocol ~= HANDLER_PROTOCOL_VERSION or type(data.revision) ~= "number" or data.revision < self.handler_revision or type(data.handler_id) ~= "string" or data.handler_id == "" then
        self.handler_sync_ready = false
        self.handler_sync_failed_revision = data.revision or 0
        self.permanent_sync_error = "protocol_mismatch: invalid handler removal"
        self:_send_sync_result(data.revision or 0, false, nil, "protocol_mismatch: invalid handler removal")
        return
    end
    if self.handler_sync_failed_revision ~= data.revision then
        self.handler_sync_failed_revision = 0
        self.update_failures = {}
    end
    self.synced_handlers[data.handler_id] = nil
    self.handler_revision = data.revision
    if data.final then
        self.handler_sync_ready = true
        local partial = next(self.update_failures) ~= nil
        self:_send_sync_result(data.revision, not partial, partial and self.update_failures or nil,
            partial and "handler_update_partially_loaded" or nil)
    end
end

function Bridge:_execute_handler(data)
    local handler = self.synced_handlers[data.handler] or self.handlers[data.handler]
    if data.handler_source then
        if type(loadstring) ~= "function" then
            return false, "client_dynamic_handlers_unsupported: loadstring is unavailable"
        end
        local compile_ok, compiled, compile_error = pcall(
            loadstring,
            data.handler_source,
            "@bridge-handler/" .. tostring(data.handler)
        )
        if not compile_ok then
            return false, "handler_compile_failed: " .. tostring(compiled)
        end
        if not compiled then
            return false, "handler_compile_failed: " .. tostring(compile_error)
        end
        local loaded, exported = pcall(compiled)
        if not loaded then
            return false, "handler_load_failed: " .. tostring(exported)
        end
        if type(exported) == "function" then
            handler = exported
        elseif type(exported) == "table" and type(exported.Run) == "function" then
            handler = function(args) return exported.Run(args) end
        elseif exported ~= nil then
            return true, exported
        else
            return false, "handler_source_invalid: handler must return a function, a table with Run, or a result"
        end
    end
    if not handler then
        if not self.handler_sync_ready then
            return false, "handler_synchronization_pending: revision " .. tostring(self.handler_revision)
        end
        return false, "unknown_handler_identifier: " .. tostring(data.handler)
    end
    local handled, response = pcall(handler, data.args)
    if not handled then
        return false, "handler_runtime_error: " .. tostring(response)
    end
    return true, response
end

function Bridge:Connect(server_url)
    assert(type(server_url) == "string" and server_url ~= "", "Server URL required")

    self.connection_generation += 1
    local generation = self.connection_generation
    self.running = true
    self.connected = false
    self.connection_state = "Connecting"
    self.retry_attempt = 0
    self.permanent_sync_error = nil
    self.url = server_url

    if self.ws then
        pcall(function() self.ws:Close() end)
        self.ws = nil
    end

    task.spawn(function()
        self:_connection_loop(generation)
    end)
end

function Bridge:_connection_loop(generation)
    while self.running and self.connection_generation == generation do
        local retryable, reason, stable = self:_connect_and_listen(generation)
        if stable then
            self.retry_attempt = 0
        end
        if not self.running or self.connection_generation ~= generation then
            return
        end
        if not retryable then
            self.running = false
            self.connected = false
            self.connection_state = "Disconnected"
            warn("[Bridge] Connection stopped: " .. tostring(reason))
            return
        end
        if self.retry_attempt >= MAX_RECONNECT_ATTEMPTS then
            self.running = false
            self.connected = false
            warn("[Bridge] Reconnection failed after " .. MAX_RECONNECT_ATTEMPTS .. " attempts: " .. tostring(reason))
            return
        end

        self.retry_attempt += 1
        self.connection_state = "Reconnecting"
        local delay = retry_delay(self.retry_attempt)
        warn(string.format(
            "[Bridge] Connection lost. Reconnecting in %d second%s (attempt %d/%d).",
            delay,
            delay == 1 and "" or "s",
            self.retry_attempt,
            MAX_RECONNECT_ATTEMPTS
        ))
        if not self:_wait_for_retry(delay, generation) then
            return
        end
    end
end

function Bridge:_wait_for_retry(delay, generation)
    local deadline = os.clock() + delay
    while os.clock() < deadline do
        if not self.running or self.connection_generation ~= generation then
            return false
        end
        task.wait(math.min(0.1, deadline - os.clock()))
    end
    return self.running and self.connection_generation == generation
end

function Bridge:_websocket_connector()
    if WebSocket and WebSocket.connect then
        return WebSocket.connect
    elseif syn and syn.websocket and syn.websocket.connect then
        return syn.websocket.connect
    elseif getrenv and getrenv().WebSocket and getrenv().WebSocket.connect then
        return getrenv().WebSocket.connect
    end
    return nil
end

function Bridge:_connect_and_listen(generation)
    local ws_connect = self:_websocket_connector()
    if not ws_connect then
        return false, "No WebSocket implementation found", false
    end

    local separator = string.find(self.url, "?", 1, true) and "&" or "?"
    local full_url = self.url .. separator .. "cid=" .. self.client_id
    local ok, ws_or_error = pcall(ws_connect, full_url)
    if not ok then
        local reason = tostring(ws_or_error)
        return not is_permanent_connect_error(reason), reason, false
    end
    local ws = ws_or_error
    if not ws then
        return true, "WebSocket connection failed", false
    end
    if self.connection_generation ~= generation or not self.running then
        pcall(function() ws:Close() end)
        return false, "Connection attempt was canceled", false
    end

    self.ws = ws
    self.connected = true
    self.connection_state = "Registering"
    self.handler_sync_ready = false
    local connected_at = os.clock()
    print("[Bridge] WebSocket connected, waiting for welcome...")

    local use_event = false
    local event_inbox = {}
    local event_connection = nil
    local event_overflow = nil
    if ws.Receive and type(ws.Receive) == "function" then
        use_event = false
    elseif ws.OnMessage then
        use_event = true
    else
        self.connected = false
        self.ws = nil
        pcall(function() ws:Close() end)
        return false, "WebSocket has no Receive/OnMessage interface", false
    end

    if use_event then
        local subscribed, connection_or_error = pcall(function()
            return ws.OnMessage:Connect(function(message)
                if type(message) ~= "string" then
                    return
                end
                if #event_inbox >= 64 then
                    event_overflow = "WebSocket message queue overflow"
                    self.connected = false
                    pcall(function() ws:Close() end)
                    return
                end
                table.insert(event_inbox, message)
            end)
        end)
        if subscribed and connection_or_error then
            event_connection = connection_or_error
        else
            -- Some older adapters expose Wait but not Connect. The server's
            -- readiness fallback still gives that path time to begin waiting.
            use_event = "wait"
        end
    end

    if ws.OnClose then
        ws.OnClose:Connect(function()
            if self.ws == ws and self.connection_generation == generation then
                self.connected = false
                print("[Bridge] WebSocket closed by server")
            end
        end)
    end

    -- The server holds its first manifest until this is sent, preventing an
    -- OnMessage-only adapter from losing frames before Wait has subscribed.
    self:_send_payload({
        type = "client_ready",
        protocol = HANDLER_PROTOCOL_VERSION
    }, "client readiness")
    print("[Bridge] WebSocket ready, waiting for welcome...")

    local welcome_received = false
    local disconnect_reason = "WebSocket connection closed"
    while self.connected and self.running and self.connection_generation == generation do
        local message = nil
        if use_event == true then
            while #event_inbox == 0 and self.connected and self.running and self.connection_generation == generation do
                task.wait()
            end
            if event_overflow then
                disconnect_reason = event_overflow
                break
            end
            if #event_inbox == 0 then
                break
            end
            message = table.remove(event_inbox, 1)
        elseif use_event == "wait" then
            local received, value = pcall(function() return ws.OnMessage:Wait() end)
            if not received or value == nil then
                disconnect_reason = received and "WebSocket returned no message" or tostring(value)
                self.connected = false
                break
            end
            message = value
        else
            local received, value = pcall(function() return ws:Receive() end)
            if not received or value == nil then
                disconnect_reason = received and "WebSocket returned no message" or tostring(value)
                self.connected = false
                break
            end
            message = value
        end

        local decoded, data = pcall(game.HttpService.JSONDecode, game.HttpService, message)
        if not decoded then
            warn("[Bridge] Invalid JSON message ignored")
            continue
        end

        if data.type == "welcome" then
            self.uuid = data.uuid
            welcome_received = true
            self.connection_state = "Synchronizing"
            print("[Bridge] Received UUID:", self.uuid)
        elseif data.type == "handler_manifest" then
            self:_apply_handler_manifest(data)
            if self.handler_sync_ready then self.connection_state = "Connected" end
        elseif data.type == "handler_update" then
            self:_apply_handler_update(data)
        elseif data.type == "handler_remove" then
            self:_apply_handler_remove(data)
        elseif data.type == "shutdown" then
            print("[Bridge] Server is shutting down - disconnecting permanently.")
            self.running = false
            self.connected = false
            disconnect_reason = "Server requested shutdown"
            pcall(function() ws:Close() end)
            break
        elseif data.type == "command" then
            if not welcome_received then
                warn("[Bridge] Received command before welcome, ignoring")
                continue
            end
            local handled, result = self:_execute_handler(data)
            self:_send_payload({
                type = "result",
                protocol = HANDLER_PROTOCOL_VERSION,
                request_id = data.request_id,
                success = handled,
                result = handled and result or nil,
                error = handled and nil or result
            }, "command result")
        end
        if self.permanent_sync_error then
            self.connected = false
            disconnect_reason = self.permanent_sync_error
            pcall(function() ws:Close() end)
            break
        end
    end

    if self.ws == ws then
        self.ws = nil
    end
    if event_connection then
        pcall(function() event_connection:Disconnect() end)
    end
    self.connected = false
    self.connection_state = "Disconnected"
    local stable = welcome_received and self.handler_sync_ready and (os.clock() - connected_at >= STABLE_CONNECTION_SECONDS)
    if self.permanent_sync_error then
        return false, self.permanent_sync_error, stable
    end
    return true, disconnect_reason, stable
end

function Bridge:Disconnect()
    self.connection_generation += 1
    self.running = false
    self.connected = false
    self.connection_state = "Closing"
    self.retry_attempt = 0
    local ws = self.ws
    self.ws = nil
    if ws then
        pcall(function() ws:Close() end)
    end
    self.connection_state = "Disconnected"
end

return Bridge.new()

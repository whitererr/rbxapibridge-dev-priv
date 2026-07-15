local Bridge = {}
Bridge.__index = Bridge

local PROTOCOL_VERSION = "1.1"
local BUILT_IN_HANDLER = "__client_info"

local function get_http_service()
    return game:GetService("HttpService")
end

local function get_player()
    while not game:IsLoaded() do task.wait(1) end
    local players = game:GetService("Players")
    local player = players.LocalPlayer
    while not player do
        task.wait(1)
        player = players.LocalPlayer
    end
    return player
end

local function avatar_url(user_id)
    local url = "https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds="
        .. tostring(user_id)
        .. "&includeBackground=false&size=420x420&format=Png&isCircular=false"

    local body = nil
    local ok = pcall(function()
        if game.HttpGet then
            body = game:HttpGet(url)
        elseif get_http_service().GetAsync then
            body = get_http_service():GetAsync(url)
        end
    end)

    if ok and body then
        local decoded_ok, payload = pcall(get_http_service().JSONDecode, get_http_service(), body)
        if decoded_ok and payload and payload.data and payload.data[1] then
            return payload.data[1].imageUrl
        end
    end

    return nil
end

function Bridge.new()
    if shared.__active_bridge and type(shared.__active_bridge.Disconnect) == "function" then
        pcall(shared.__active_bridge.Disconnect, shared.__active_bridge)
        print("[Bridge] Stopped previous bridge instance.")
    end

    local self = setmetatable({}, Bridge)
    self.handlers = {}
    self.assigned_handlers = {}
    self.uuid = nil
    self.url = nil
    self.ws = nil
    self.connected = false
    self.reconnect_delay = 5
    self.running = false

    local player = get_player()
    self.player = player
    self.client_id = tostring(player.UserId)

    self:_install_built_in_handlers()
    shared.__active_bridge = self

    return self
end

function Bridge:_install_built_in_handlers()
    self.handlers[BUILT_IN_HANDLER] = function()
        local player = self.player or get_player()
        return {
            username = player.Name,
            display_name = player.DisplayName,
            user_id = player.UserId,
            avatar_image_url = avatar_url(player.UserId),
            place_id = game.PlaceId,
            job_id = game.JobId,
        }
    end
end

function Bridge:_client_info()
    local ok, info = pcall(self.handlers[BUILT_IN_HANDLER])
    if ok then
        return info
    end
    return {
        username = "Unknown",
        user_id = tonumber(self.client_id) or self.client_id,
        avatar_image_url = avatar_url(self.client_id),
        place_id = game.PlaceId,
        error = tostring(info),
    }
end

function Bridge:On(handler_name, callback)
    self.handlers[handler_name] = callback
    self.assigned_handlers[handler_name] = true
end

function Bridge:Off(handler_name)
    if handler_name == BUILT_IN_HANDLER then
        return
    end
    self.handlers[handler_name] = nil
    self.assigned_handlers[handler_name] = nil
end

function Bridge:Connect(server_url)
    assert(server_url, "Server URL required")
    self.url = server_url
    self.running = true
    task.spawn(function()
        while self.running do
            self:_connect_and_listen()
            if not self.running then break end
            task.wait(self.reconnect_delay)
        end
    end)
end

function Bridge:_websocket_connect()
    if WebSocket and WebSocket.connect then
        return WebSocket.connect
    elseif syn and syn.websocket and syn.websocket.connect then
        return syn.websocket.connect
    elseif getrenv and getrenv().WebSocket and getrenv().WebSocket.connect then
        return getrenv().WebSocket.connect
    end
    return nil
end

function Bridge:_send(payload)
    local encoded = get_http_service():JSONEncode(payload)
    pcall(function()
        if self.ws then
            self.ws:Send(encoded)
        end
    end)
end

function Bridge:_ack(handler_id, status, message)
    self:_send({
        type = "handler_ack",
        handler_id = handler_id,
        status = status,
        message = message,
    })
end

function Bridge:_load_handler(record)
    local handler_id = tostring(record.id or record.name or "")
    local source = record.source
    if handler_id == "" or type(source) ~= "string" then
        self:_ack(handler_id, "error", "Invalid handler payload")
        return
    end

    local chunk, compile_error = loadstring(source)
    if not chunk then
        self:_ack(handler_id, "error", "Compile error: " .. tostring(compile_error))
        return
    end

    local env = {
        Bridge = self,
        game = game,
        workspace = workspace,
        script = script,
        shared = shared,
        task = task,
        wait = task.wait,
        print = print,
        warn = warn,
        error = error,
        assert = assert,
        pcall = pcall,
        xpcall = xpcall,
        tostring = tostring,
        tonumber = tonumber,
        type = type,
        pairs = pairs,
        ipairs = ipairs,
        next = next,
        select = select,
        table = table,
        string = string,
        math = math,
        os = os,
        coroutine = coroutine,
        Vector3 = Vector3,
        CFrame = CFrame,
        Color3 = Color3,
        Instance = Instance,
        Enum = Enum,
    }

    if setfenv then
        pcall(setfenv, chunk, setmetatable(env, { __index = getfenv and getfenv(0) or _G }))
    end

    local ok, result = pcall(chunk, self)
    if not ok then
        self:_ack(handler_id, "error", "Load error: " .. tostring(result))
        return
    end

    if type(result) == "function" then
        self.handlers[handler_id] = result
    elseif type(self.handlers[handler_id]) ~= "function" then
        self:_ack(handler_id, "error", "Handler source must return a function or call Bridge:On")
        return
    end

    self.assigned_handlers[handler_id] = true
    self:_ack(handler_id, "ok", "loaded")
end

function Bridge:_replace_manifest(records)
    for handler_id in pairs(self.assigned_handlers) do
        self:Off(handler_id)
    end

    for _, record in ipairs(records or {}) do
        self:_load_handler(record)
    end

    self:_ack("manifest", "ok", "synced")
end

function Bridge:_handle_command(data)
    local handler = self.handlers[data.handler]
    local result = nil
    if not handler then
        result = "No handler: " .. tostring(data.handler)
    else
        local ok, response = pcall(handler, data.args or {})
        if ok then
            result = response
        else
            result = "Handler error: " .. tostring(response)
            warn("[Bridge] Handler error:", response)
        end
    end

    self:_send({
        type = "result",
        request_id = data.request_id,
        result = result,
    })
end

function Bridge:_connect_and_listen()
    local ws_connect = self:_websocket_connect()
    if not ws_connect then
        warn("[Bridge] No WebSocket implementation found!")
        self.running = false
        return
    end

    local separator = string.find(self.url, "?", 1, true) and "&" or "?"
    local full_url = self.url .. separator .. "cid=" .. self.client_id .. "&protocol=" .. PROTOCOL_VERSION
    local connect_ok, ws = pcall(ws_connect, full_url)
    if not connect_ok or not ws then
        warn("[Bridge] WebSocket connection failed:", ws)
        return
    end

    self.ws = ws
    self.connected = true
    print("[Bridge] WebSocket connected, waiting for welcome...")

    local use_event = false
    if ws.Receive and type(ws.Receive) == "function" then
        use_event = false
    elseif ws.OnMessage then
        use_event = true
    else
        warn("[Bridge] WebSocket has no Receive/OnMessage")
        self.connected = false
        pcall(function() ws:Close() end)
        return
    end

    if ws.OnClose then
        ws.OnClose:Connect(function()
            self.connected = false
            print("[Bridge] WebSocket closed by server")
        end)
    end

    local welcome_received = false
    while self.connected and self.running do
        local message = nil
        if use_event then
            local ok, msg = pcall(function() return ws.OnMessage:Wait() end)
            if not ok or msg == nil then
                self.connected = false
                break
            end
            message = msg
        else
            local ok, msg = pcall(function() return ws:Receive() end)
            if not ok or msg == nil then
                self.connected = false
                break
            end
            message = msg
        end

        local ok, data = pcall(get_http_service().JSONDecode, get_http_service(), message)
        if not ok then
            warn("[Bridge] Invalid JSON:", message)
            continue
        end

        if data.type == "welcome" then
            self.uuid = data.uuid
            welcome_received = true
            self:_send({
                type = "hello",
                protocol = PROTOCOL_VERSION,
                info = self:_client_info(),
            })
            print("[Bridge] Received UUID:", self.uuid)
        elseif data.type == "shutdown" then
            print("[Bridge] Server is shutting down - disconnecting permanently.")
            self.running = false
            self.connected = false
            pcall(function() self.ws:Close() end)
            break
        elseif data.type == "handler_manifest" then
            self:_replace_manifest(data.handlers)
        elseif data.type == "handler_update" then
            self:_load_handler(data.handler or {})
        elseif data.type == "handler_remove" then
            self:Off(tostring(data.handler_id))
            self:_ack(tostring(data.handler_id), "ok", "removed")
        elseif data.type == "handler_clear" then
            self:_replace_manifest({})
        elseif data.type == "command" then
            if not welcome_received then
                warn("[Bridge] Received command before welcome, ignoring")
                continue
            end
            self:_handle_command(data)
        end
    end

    self.ws = nil
    if self.running then
        print("[Bridge] WebSocket disconnected. Reconnecting in " .. self.reconnect_delay .. "s...")
    end
end

function Bridge:Disconnect()
    self.running = false
    self.connected = false
    if self.ws then
        pcall(function() self.ws:Close() end)
    end
end

return Bridge.new()

local http = require("socket.http")
local ltn12 = require("ltn12")
local json = require("dkjson")

local rollup_server = assert(os.getenv("ROLLUP_HTTP_SERVER_URL"), "missing ROLLUP_HTTP_SERVER_URL")

local function info(...)
    print(string.format(...))
end

local function http_post(url, body)
    local request_body = json.encode(body)
    local response_body = {}
    local result, code = http.request {
        method = "POST",
        url = url,
        source = ltn12.source.string(request_body),
        headers = {
            ["Content-Type"] = "application/json",
            ["Content-Length"] = #request_body
        },
        sink = ltn12.sink.table(response_body)
    }
    if result == nil then error("HTTP POST Request to " .. url .. " failed. " .. code) end
    return code, table.concat(response_body)
end

local handlers = {}
function handlers.advance_state(data)
    info("Received advance request data %s", json.encode(data))
    local lambada_server_addr = os.getenv("LAMBADA_HTTP_SERVER_URL")
    if lambada_server_addr then
        local response, err

        response, err = http.request {
            method = "GET",
            url = lambada_server_addr .. "/open_state",
            headers = { ["Content-Type"] = "application/json" },
        }
        if not response then
            return "Failed to open state: " .. err
        end
        print("State opened successfully.")

        local request_body = "hello world"

        response, err = http.request {
            method = "POST",
            url = lambada_server_addr .. "/set_state/output",
            headers = { ["Content-Type"] = "application/octet-stream" },
            source = ltn12.source.string(request_body),
        }
        if not response then
            return "Failed to set state: " .. err
        end
        print("State set successfully.")

        response, err = http.request {
            method = "GET",
            url = lambada_server_addr .. "/commit_state",
            headers = { ["Content-Type"] = "application/json" },
        }
        if not response then
            return "Failed to commit state: " .. err
        end
        print("State committed successfully.")
    end
    return "accept"
end

function handlers.inspect_state(data)
    info("Received inspect request data %s", json.encode(data))
    -- TODO: add application code here
    return "accept"
end

local mt = {__index = function(t, k) error("Invalid request type: " .. k) end}
setmetatable(handlers, mt)

local finish = {status = "accept"}
while true do
    info("Sending finish")
    local code, response = http_post(rollup_server .. "/finish", finish)
    info("Received finish status %d", code)
    if code == 202 then
        info("No pending rollup request, trying again")
    else
        local rollup_request = json.decode(response)
        local metadata = rollup_request.data.metadata
        finish.status = handlers[rollup_request.request_type](rollup_request.data)
    end
end

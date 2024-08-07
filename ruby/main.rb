require 'json'
require 'http'

def log(message)
  puts message
end

def handle_advance(data)
  log("Received advance request data #{data}")
  lambada_server_url = ENV['LAMBADA_HTTP_SERVER_URL']

  if lambada_server_url

    response_open_state = HTTP.get("#{lambada_server_url}/open_state", {
      headers: {
      'Content-Type': 'application/json'
      },
    });
  
    if !response_open_state.status.success?
      return "Failed to open state: #{response_open_state.code}"
    end
    log("State opened successfully.")

    response_set_state = HTTP.post("#{lambada_server_url}/set_state/output", {
      body: 'hello world',
      headers: {
      'Content-Type': 'application/octet-stream'
      },
    });
    
    if !response_set_state.status.success?
      return "Failed to set state: #{response_set_state.code}"
    end
    log("State set successfully.")

    response_commit_state = HTTP.get("#{lambada_server_url}/commit_state", {
      headers: {
      'Content-Type': 'application/json'
      },
    });
  
    if !response_commit_state.status.success?
      return "Failed to commit state: #{response_commit_state.code}"
    end
    log("State committed successfully.")
  end
  
  'accept'
end

def handle_inspect(data)
  log("Received inspect request data #{data}");
  payload = data['payload']
  # TODO: add application logic here
  return "accept"
end

ROLLUP_SERVER = ENV.fetch('ROLLUP_HTTP_SERVER_URL', 'http://127.0.0.1:5004')
log("HTTP rollup_server url is #{ROLLUP_SERVER}")

finish = { status: "accept" }

while (true) do
  log("Sending finish")

  response = HTTP.post(ROLLUP_SERVER + '/finish', {
    headers: {
      'Content-Type': 'application/json'
    },
    json: { status: 'accept' }
  });

  log("Received finish status #{response.status}")

  if response.status == 202
    log("No pending rollup request, trying again")
  else
    rollup_req = response.parse
    metadata = rollup_req['data']['metadata']
    case rollup_req['request_type']
    when 'advance_state'
      finish[:status] = handle_advance(rollup_req['data'])
    when 'inspect_state'
      finish[:status] = handle_inspect(rollup_req['data'])
    end
  end
end
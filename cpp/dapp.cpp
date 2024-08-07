#include <stdio.h>
#include <iostream>

#include "3rdparty/cpp-httplib/httplib.h"
#include "3rdparty/picojson/picojson.h"

std::string handle_advance(httplib::Client &cli, picojson::value data)
{
    std::cout << "Received advance request data " << data << std::endl;
    const char* lambada_server_url = std::getenv("LAMBADA_HTTP_SERVER_URL");

    if(lambada_server_url) {
        httplib::Client cli(lambada_server_url);
        auto response_open_state = cli.Get("/open_state", {{"Content-Type","application/json"}});
        if (!response_open_state) {
            return "Failed to open state: " + std::to_string(response_open_state ? response_open_state->status : 0);
        }
        std::cout << "State opened successfully." << std::endl;

        auto response_set_state = cli.Post("/set_state/output", "hello world", "application/octet-stream");
        if (!response_set_state) {
            return "Failed to set state: " + std::to_string(response_set_state ? response_set_state->status : 0);
        }
        std::cout << "State set successfully." << std::endl;

        auto response_commit_state = cli.Get("/commit_state", {{"Content-Type","application/json"}});
        if (!response_commit_state) {
            return "Failed to commit state: " + std::to_string(response_commit_state ? response_commit_state->status : 0);
        }
        std::cout << "State committed successfully." << std::endl;
    }
    return "accept";
}

std::string handle_inspect(httplib::Client &cli, picojson::value data)
{
    std::cout << "Received inspect request data " << data << std::endl;
    return "accept";
}

int main(int argc, char **argv)
{
    std::map<std::string, decltype(&handle_advance)> handlers = {
        {std::string("advance_state"), &handle_advance},
        {std::string("inspect_state"), &handle_inspect},
    };
    httplib::Client cli(getenv("ROLLUP_HTTP_SERVER_URL"));
    cli.set_read_timeout(20, 0);
    std::string status("accept");
    std::string rollup_address;
    while (true)
    {
        std::cout << "Sending finish" << std::endl;
        auto finish = std::string("{\"status\":\"") + status + std::string("\"}");
        auto r = cli.Post("/finish", finish, "application/json");
        std::cout << "Received finish status " << r.value().status << std::endl;
        if (r.value().status == 202)
        {
            std::cout << "No pending rollup request, trying again" << std::endl;
        }
        else
        {
            picojson::value rollup_request;
            picojson::parse(rollup_request, r.value().body);
            picojson::value metadata = rollup_request.get("data").get("metadata");
            auto request_type = rollup_request.get("request_type").get<std::string>();
            auto handler = handlers.find(request_type)->second;
            auto data = rollup_request.get("data");
            status = (*handler)(cli, data);
        }
    }
    return 0;
}

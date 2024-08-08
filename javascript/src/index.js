// XXX even though ethers is not used in the code below, it's very likely
// it will be used by any DApp, so we are already including it here
const { ethers } = require("ethers");

const rollup_server = process.env.ROLLUP_HTTP_SERVER_URL;
console.log("HTTP rollup_server url is " + rollup_server);
const lambadaServer = process.env.LAMBADA_HTTP_SERVER_URL;
console.log("Lambada server url is " + lambadaServer);

async function handle_advance(data) {
  console.log("Received advance request data " + JSON.stringify(data));

  // Make a GET request to open_state endpoint
  if (lambadaServer) {
    const openStateResponse = await fetch(`${lambadaServer}/open_state`);
    // Optional: Check if the request was successful
    if (!openStateResponse.ok) {
      throw new Error(
        `Failed to open state: ${openStateResponse.status} ${openStateResponse.statusText}`
      );
    }
    console.log("State opened successfully.");
  }
  
  if (lambadaServer) {
    const setStateResponse = await fetch(`${lambadaServer}/set_state/output`, {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
      },
      body: 'hello world'
    });
    // Optional: Check if the request was successful
    if (!setStateResponse.ok) {
      throw new Error(
        `Failed to set state: ${setStateResponse.status} ${setStateResponse.statusText}`
      );
    }
    console.log("State set successfully.");
  }

  // unless something happens we will commit in the end, else we cause an exception
  
  // Make a GET request to commit_state endpoint if we have a lambada server
  if (lambadaServer) {
    const commitStateResponse = await fetch(`${lambadaServer}/commit_state`);
    // Optional: Check if the request was successful
    if (!commitStateResponse.ok) {
      throw new Error(
        `Failed to commit state: ${commitStateResponse.status} ${commitStateResponse.statusText}`
      );
    }
    // This will never show as we did the job and the runtime stopped us
    console.log("State committed successfully.");
  }
  return "accept";
}

async function handle_inspect(data) {
  console.log("Received inspect request data " + JSON.stringify(data));
  return "accept";
}

var handlers = {
  advance_state: handle_advance,
  inspect_state: handle_inspect,
};

var finish = { status: "accept" };

(async () => {
  while (true) {
    const finish_req = await fetch(rollup_server + "/finish", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ status: "accept" }),
    });

    console.log("Received finish status " + finish_req.status);

    if (finish_req.status == 202) {
      console.log("No pending rollup request, trying again");
    } else {
      const rollup_req = await finish_req.json();
      var handler = handlers[rollup_req["request_type"]];
      finish["status"] = await handler(rollup_req["data"]);
    }
  }
})();

import createClient from "openapi-fetch";
import { components, paths } from "./schema";
import process from "process";
import crypto from "crypto";

type AdvanceRequestData = components["schemas"]["Advance"];
type RequestHandlerResult = components["schemas"]["Finish"]["status"];
type RollupsRequest = components["schemas"]["RollupRequest"];
type AdvanceRequestHandler = (
  data: AdvanceRequestData
) => Promise<RequestHandlerResult>;

const apiUrl = process.env.IPFS_API || "http://127.0.0.1:5001";

const rollupServer = process.env.ROLLUP_HTTP_SERVER_URL;
console.log("HTTP rollup_server url is " + rollupServer);

const lambadaServer = process.env.LAMBADA_HTTP_SERVER_URL;
console.log("Lambada server url is " + lambadaServer);

const handleAdvance: AdvanceRequestHandler = async (
  data: AdvanceRequestData
) => {
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
};

const main = async () => {
  const { POST } = createClient<paths>({ baseUrl: rollupServer });
  let status: RequestHandlerResult = "accept";
  while (true) {
    const { response } = await POST("/finish", {
      body: { status },
      parseAs: "text",
    });

    if (response.status === 200) {
      const data = (await response.json()) as RollupsRequest;
      switch (data.request_type) {
        case "advance_state":
          status = await handleAdvance(data.data as AdvanceRequestData);
          break;
      }
      // there is no inspect state!
    } else if (response.status === 202) {
      console.log(await response.text());
    }
  }
};

main().catch((e) => {
  console.log(e);
  process.exit(1);
});

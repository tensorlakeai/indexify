import { Box, Alert } from "@mui/material";
import { ComputeGraphsList, IndexifyClient } from "getindexify";
import { useLoaderData } from "react-router-dom";
import ComputeGraphs from "../../components/ComputeGraphs";

const ComputeGraphsPage = () => {
  const { client, computeGraphs, namespace } = useLoaderData() as {
    client: IndexifyClient;
    computeGraphs: ComputeGraphsList;
    namespace: string;
  };

  if (!client || !computeGraphs || !namespace) {
    return (
      <Box>
        <Alert severity="error">Failed to load compute graphs data. Please try again.</Alert>
      </Box>
    );
  }

  return (
    <Box>
      <ComputeGraphs
        computeGraphs={computeGraphs}
        client={client}
        namespace={namespace}
      />
    </Box>
  );
};

export default ComputeGraphsPage;

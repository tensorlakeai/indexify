import { Box } from "@mui/material";
import { ExtractionGraph, IndexifyClient } from "getindexify";
import { useLoaderData } from "react-router-dom";
import ExtractionGraphs from "../../components/ExtractionGraphs";

const ExtractionGraphsPage = () => {
  const { client, namespace } = useLoaderData() as {
    client: IndexifyClient;
    extractionGraphs: ExtractionGraph[];
    namespace: string;
  };
  return (
    <Box>
      <ExtractionGraphs
        client={client}
        namespace={namespace}
      />
    </Box>
  );
};

export default ExtractionGraphsPage;

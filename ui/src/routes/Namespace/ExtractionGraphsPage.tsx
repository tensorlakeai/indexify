import { Box } from "@mui/material";
import { ExtractionGraph } from "getindexify";
import { useLoaderData } from "react-router-dom";
import ExtractionGraphs from "../../components/ExtractionGraphs";
import { Extractor } from "getindexify";

const ExtractionGraphsPage = () => {
  const { extractors, extractionGraphs, namespace } = useLoaderData() as {
    extractionGraphs: ExtractionGraph[];
    namespace: string;
    extractors: Extractor[];
  };
  return (
    <Box>
      <ExtractionGraphs
        extractors={extractors}
        namespace={namespace}
        extractionGraphs={extractionGraphs}
        tasks={[]}
      />
    </Box>
  );
};

export default ExtractionGraphsPage;

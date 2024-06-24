import { Box } from "@mui/material";
import { ExtractionGraph } from "getindexify";
import { useLoaderData } from "react-router-dom";
import ExtractionGraphs from "../../components/ExtractionGraphs";
import { Extractor } from "getindexify";
import { TaskCountsMap } from "../../types";

const ExtractionGraphsPage = () => {
  const { extractors, extractionGraphs, namespace, taskCountsMap } = useLoaderData() as {
    extractionGraphs: ExtractionGraph[];
    namespace: string;
    extractors: Extractor[];
    taskCountsMap: TaskCountsMap
  };
  return (
    <Box>
      <ExtractionGraphs
        extractors={extractors}
        namespace={namespace}
        extractionGraphs={extractionGraphs}
        taskCountsMap={taskCountsMap}
      />
    </Box>
  );
};

export default ExtractionGraphsPage;

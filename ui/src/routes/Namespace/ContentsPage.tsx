import { Box } from "@mui/material";
import { IndexifyClient } from "getindexify";
import { useLoaderData } from "react-router-dom";
import ContentTable from "../../components/tables/ContentTable";
import { IContentMetadataExtended } from "../../types";

const ContentsPage = () => {
  const { client } = useLoaderData() as {
    client: IndexifyClient;
  };

  const contentLoader = async ({
    parentId,
    startId,
    pageSize
  }: {
    parentId?: string;
    startId?: string;
    pageSize: number;
  }): Promise<IContentMetadataExtended[]> => {
    const contentList = await client.getExtractedContent({
      parentId,
      startId,
      limit: pageSize + 1
    });

    //count children
    return Promise.all(
      contentList.map(async content => {
        const tree = await client.getContentTree(content.id);
        return {
          ...content,
          children: tree.filter(c => c.parent_id === content.id).length
        };
      })
    );
  };

  return (
    <Box>
      <ContentTable loadData={contentLoader} client={client} />
    </Box>
  );
};

export default ContentsPage;

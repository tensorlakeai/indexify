import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IContentMetadata } from "../lib/Indexify/types";
import { Alert, Button, Tab, Tabs, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import ArticleIcon from "@mui/icons-material/Article";
import React, { useState } from "react";
import moment from "moment";
import { Link } from "react-router-dom";

function getChildCountMap(
  contents: IContentMetadata[]
): Record<string, number> {
  // Initialize a record to hold the count of children for each parent
  const childrenCountMap: Record<string, number> = {};
  // iterate over content
  contents.forEach((content) => {
    if (content.parent_id) {
      if (childrenCountMap[content.parent_id]) {
        childrenCountMap[content.parent_id]++;
      } else {
        childrenCountMap[content.parent_id] = 1;
      }
    }
  });

  const result: Record<string, number> = {};
  contents.forEach((content) => {
    result[content.id] = childrenCountMap[content.id] || 0;
  });

  return result;
}

const ContentTable = ({ content }: { content: IContentMetadata[] }) => {
  const childCount = getChildCountMap(content);

  const [currentFilterId, setCurrentFilterId] = useState<string | null>(null);
  const [filteredContent, setFilteredContent] = useState(content.filter((c) => c.source === "ingestion"));
  const [currentTab, setCurrentTab] = useState("ingested");


  const onClickFilterId = (selectedContent: IContentMetadata) => {
    const newFilteredContent = [
      ...content.filter((c) => c.parent_id === selectedContent.id),
    ];
    if (newFilteredContent.length === 0) {
      alert("no content");
      return;
    }
    //update filterred content
    setFilteredContent(newFilteredContent);
    setCurrentFilterId(selectedContent.id);
    setCurrentTab("currentid");
  };

  const filterIngested = () => {
    setFilteredContent([...content.filter((c) => c.source === "ingestion")]);
  };

  const resetTabs = () => {
    setFilteredContent(content);
    setCurrentFilterId(null);
  };

  const onChangeTab = (event: React.SyntheticEvent, selectedValue: string) => {
    setCurrentTab(selectedValue);
    if (selectedValue === "all") {
      resetTabs();
    } else if (selectedValue === "ingested") {
      setCurrentFilterId(null);
      filterIngested();
    }
  };

  const columns: GridColDef[] = [
    {
      field: "view",
      headerName: "",
      width: 100,
      renderCell: (params) => (
        <Link to={`/${params.row.namespace}/content/${params.row.id}`}>
          <Button sx={{ p: 0.5 }} variant="outlined">
            View
          </Button>
        </Link>
      ),
    },
    {
      field: "id",
      headerName: "ID",
      width: 170,
      renderCell: (params) => {
        const clickable =
          currentTab !== "all" && childCount[params.row.id] !== 0;
        return (
          <Button
            onClick={() => onClickFilterId(params.row)}
            sx={{
              pointerEvents: clickable ? "all" : "none",
              textDecoration: clickable ? "underline" : "none",
            }}
            variant="text"
          >
            <Typography variant="body1">{params.value}</Typography>
          </Button>
        );
      },
    },
    {
      field: "childCount",
      headerName: "Children",
      width: 140,
      valueGetter: (params) => childCount[params.row.id],
    },
    {
      field: "name",
      headerName: "Name",
      width: 200,
    },
    {
      field: "parent_id",
      headerName: "Parent ID",
      width: 200,
    },
    {
      field: "content_type",
      headerName: "ContentType",
      width: 150,
    },
    {
      field: "source",
      headerName: "Source",
      width: 140,
    },
    {
      field: "labels",
      headerName: "Labels",
      width: 300,
      valueGetter: (params) => {
        return JSON.stringify(params.value);
      },
    },
    {
      field: "storage_url",
      headerName: "Storage URL",
      width: 200,
    },
    {
      field: "created_at",
      headerName: "Created At",
      width: 200,
      valueGetter: (params) => {
        return moment(params.value * 1000).format("MM/DD/YYYY h:mm A");
      },
    },
  ];

  const renderContent = () => {
    if (content.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Content Found
          </Alert>
        </Box>
      );
    }

    return (
      <Box sx={{ width: "100%" }}>
        <DataGrid
          sx={{ backgroundColor: "white" }}
          autoHeight
          rows={filteredContent}
          columns={columns}
          initialState={{
            pagination: {
              paginationModel: { page: 0, pageSize: 5 },
            },
          }}
          pageSizeOptions={[5, 10]}
        />
      </Box>
    );
  };

  return (
    <>
      <Stack
        display={"flex"}
        direction={"row"}
        alignItems={"center"}
        spacing={2}
      >
        <ArticleIcon />
        <Typography variant="h3">Content</Typography>
      </Stack>
      <Box>
        <Tabs
          value={currentTab}
          onChange={onChangeTab}
          aria-label="disabled tabs example"
        >
          <Tab value={"all"} label="All" />
          <Tab value={"ingested"} label="Ingested" />

          {currentFilterId !== null && (
            <Tab value={"currentid"} label={currentFilterId} />
          )}
        </Tabs>
      </Box>
      {renderContent()}
    </>
  );
};

export default ContentTable;

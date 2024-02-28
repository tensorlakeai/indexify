import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IContentMetadata } from "getindexify";
import {
  Alert,
  Button,
  Input,
  Tab,
  Tabs,
  TextField,
  Typography,
} from "@mui/material";
import { Box, Stack } from "@mui/system";
import ArticleIcon from "@mui/icons-material/Article";
import React, { useEffect, useState } from "react";
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

const ContentTable = ({
  namespace,
  content,
}: {
  namespace: string;
  content: IContentMetadata[];
}) => {
  const childCount = getChildCountMap(content);

  const [paginationModel, setPaginationModel] = useState({
    page: 1,
    pageSize: 5,
  });
  const [filterIds, setFilterIds] = useState<string[]>([]);
  const [searchFilter, setSearchFilter] = useState<{
    contentId: string;
  }>({ contentId: "" });

  const [filteredContent, setFilteredContent] = useState(
    content.filter((c) => c.source === "ingestion")
  );
  const [currentTab, setCurrentTab] = useState("ingested");

  const updateTableContentByFilter = (id: string) => {
    const newFilteredContent = [...content.filter((c) => c.parent_id === id)];
    setFilteredContent(newFilteredContent);
  };

  const onClickChildren = (selectedContent: IContentMetadata) => {
    // append id to filterIds - this adds a new tab
    updateTableContentByFilter(selectedContent.id);
    setFilterIds([...filterIds, selectedContent.id]);
    setCurrentTab(selectedContent.id);
  };

  const onChangeTab = (event: React.SyntheticEvent, selectedValue: string) => {
    setCurrentTab(selectedValue);
    if (selectedValue === "search") {
      resetFilters();
    } else if (selectedValue === "ingested") {
      setFilterIds([]);
      setFilteredContent([...content.filter((c) => c.source === "ingestion")]);
    } else {
      // click previous filter tab
      // remove tabs after id: selectedValue
      const index = filterIds.indexOf(selectedValue);
      const newIds = [...filterIds];
      newIds.splice(index + 1);
      setFilterIds(newIds);
      updateTableContentByFilter(selectedValue);
    }
  };

  const resetFilters = () => {
    setFilteredContent(content);
    setSearchFilter({ contentId: "" });
  };

  const goToPage = (page: number) => {
    setPaginationModel((currentModel) => ({
      ...currentModel,
      page,
    }));
  };

  useEffect(() => {
    // when filtered content updates go to first page
    goToPage(0);
  }, [filteredContent]);

  useEffect(() => {
    // when we update searchFilter update content
    if (currentTab !== "search") return;
    goToPage(0);
    setFilteredContent(
      content.filter((c) => c.id.startsWith(searchFilter.contentId))
    );
  }, [searchFilter]);

  const columns: GridColDef[] = [
    {
      field: "view",
      headerName: "",
      width: 100,
      renderCell: (params) => (
        <Link
          to={`/${params.row.namespace}/content/${params.row.id}`}
          target="_blank"
        >
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
    },
    {
      field: "childCount",
      headerName: "Children",
      width: 140,
      valueGetter: (params) => childCount[params.row.id],
      renderCell: (params) => {
        const clickable =
          currentTab !== "search" && childCount[params.row.id] !== 0;
        return (
          <Button
            onClick={(e) => {
              e.stopPropagation();
              onClickChildren(params.row);
            }}
            sx={{
              pointerEvents: clickable ? "search" : "none",
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
      field: "name",
      headerName: "Name",
      width: 200,
    },
    {
      field: "source",
      headerName: "Source",
      width: 140,
    },
    {
      field: "content_type",
      headerName: "ContentType",
      width: 150,
    },
    {
      field: "parent_id",
      headerName: "Parent ID",
      width: 170,
    },
    {
      field: "labels",
      headerName: "Labels",
      width: 170,
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
          paginationModel={paginationModel}
          onPaginationModelChange={setPaginationModel}
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
      <Box justifyContent={"space-between"} display={"flex"}>
        <Tabs
          value={currentTab}
          onChange={onChangeTab}
          aria-label="disabled tabs example"
        >
          <Tab value={"search"} label="Search" />
          <Tab value={"ingested"} label="Ingested" />

          {filterIds.map((id, i) => {
            return <Tab key={`filter-${id}`} value={id} label={id} />;
          })}
        </Tabs>
        {/* Filter for search tab */}
        {currentTab === "search" && (
          <Box>
            <TextField
              onChange={(e) => {
                setSearchFilter({ ...searchFilter, contentId: e.target.value });
              }}
              value={searchFilter.contentId}
              size="small"
              label="Content Id"
            />
          </Box>
        )}
      </Box>
      {renderContent()}
    </>
  );
};

export default ContentTable;

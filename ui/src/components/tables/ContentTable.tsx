import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IContentMetadata, IExtractionPolicy } from "getindexify";
import {
  Alert,
  Button,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
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
  extractionPolicies,
  content,
}: {
  extractionPolicies: IExtractionPolicy[];
  content: IContentMetadata[];
}) => {
  const childCountMap = getChildCountMap(content);
  const [paginationModel, setPaginationModel] = useState({
    page: 1,
    pageSize: 5,
  });
  const [graphTabIds, setGraphTabIds] = useState<string[]>([]);
  const [searchFilter, setSearchFilter] = useState<{
    contentId: string;
    policyName: string;
  }>({ contentId: "", policyName: "Any" });

  const [filteredContent, setFilteredContent] = useState(
    content.filter((c) => c.source === "ingestion")
  );
  const [currentTab, setCurrentTab] = useState("ingested");

  const onClickChildren = (selectedContent: IContentMetadata) => {
    // append id to graphTabIds - this adds a new tab
    setGraphTabIds([...graphTabIds, selectedContent.id]);
    setCurrentTab(selectedContent.id);
  };

  const onChangeTab = (event: React.SyntheticEvent, selectedValue: string) => {
    setCurrentTab(selectedValue);
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
    if (currentTab === "search") {
      goToPage(0);
      const searchPolicy = extractionPolicies.find(
        (policy) => policy.name === searchFilter.policyName
      );
      setFilteredContent(
        content.filter((c) => {
          // filter contentId
          if (!c.id.startsWith(searchFilter.contentId)) return false;
          // filter policy
          if (searchFilter.policyName === "Ingested") {
            // TODO: match mimetype and filter ingested source
          } else if (searchPolicy && searchFilter.policyName !== "Any") {
            if (c.source !== searchPolicy?.name) {
              return false;
            }
          }
          return true;
        })
      );
    } else if (currentTab === "ingested") {
      // go back to root node of graph tab
      setGraphTabIds([]);
      setFilteredContent([...content.filter((c) => c.source === "ingestion")]);
    } else {
      // current tab is now a content id
      // remove tabs after id: selectedValue if possible

      setGraphTabIds((currentIds) => {
        const index = currentIds.indexOf(currentTab);
        const newIds = [...currentIds];
        newIds.splice(index + 1);
        return newIds;
      });
      // update filteredContent
      const newFilteredContent = [
        ...content.filter((c) => c.parent_id === currentTab),
      ];
      setFilteredContent(newFilteredContent);
    }
  }, [searchFilter, currentTab, content, extractionPolicies]);

  let columns: GridColDef[] = [
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
      valueGetter: (params) => childCountMap[params.row.id],
      renderCell: (params) => {
        const clickable =
          currentTab !== "search" && childCountMap[params.row.id] !== 0;
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
      field: "source",
      headerName: "Source",
      width: 220,
    },
    {
      field: "mime_type",
      headerName: "Mime Type",
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
      width: 150,
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

  columns = columns.filter((col) => {
    if (
      currentTab === "ingested" &&
      (col.field === "source" || col.field === "parent_id")
    ) {
      return false;
    }
    return true;
  });

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
          initialState={{
            sorting: {
              sortModel: [{ field: "created_at", sort: "desc" }],
            },
          }}
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

          {graphTabIds.map((id, i) => {
            return <Tab key={`filter-${id}`} value={id} label={id} />;
          })}
        </Tabs>
        {/* Filter for search tab */}
        {currentTab === "search" && (
          <Box display="flex" gap={2}>
            {/* Added gap for spacing between elements */}
            <TextField
              onChange={(e) =>
                setSearchFilter({
                  ...searchFilter,
                  contentId: e.target.value,
                })
              }
              value={searchFilter.contentId}
              label="Content Id"
              sx={{ width: "auto" }} // Adjust width as needed
              size="small"
            />
            <FormControl sx={{ minWidth: 200 }} size="small">
              <InputLabel id="demo-select-small-label">
                Extraction Policy
              </InputLabel>
              <Select
                labelId="demo-select-small-label"
                id="demo-select-small"
                label="Extraction Policy"
                value={searchFilter.policyName}
                onChange={(e) =>
                  setSearchFilter({
                    ...searchFilter,
                    policyName: e.target.value,
                  })
                }
              >
                <MenuItem value="Any">Any</MenuItem>
                {/* <MenuItem value="Ingested">Ingested</MenuItem> */}
                {extractionPolicies.map((policy) => (
                  <MenuItem key={policy.name} value={policy.name}>
                    {policy.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>
        )}
      </Box>
      {renderContent()}
    </>
  );
};

export default ContentTable;

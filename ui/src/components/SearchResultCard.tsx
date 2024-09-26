import React, { useState } from "react";
import { Box, Chip, Divider, Paper, Typography } from "@mui/material";
import { ISearchIndexResponse, IContentMetadata, IndexifyClient } from "getindexify";
import ContentDrawer from "./ContentDrawer";

const DisplayData = ({
  label,
  value
}: {
  label: string;
  value: string | number;
}) => {
  return (
    <Box display="flex" flexDirection={"row"} alignItems={"center"}>
      <Typography sx={{ fontSize: "14px" }} variant="body1">
        {label}:{" "}
      </Typography>
      <Typography sx={{ fontSize: "14px" }} pl={1} variant="label">
        {value}
      </Typography>
    </Box>
  );
};

const SearchResultCard = ({
  data,
  namespace,
  client
}: {
  data: ISearchIndexResponse;
  namespace: string;
  client: IndexifyClient;
}) => {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [selectedContent, setSelectedContent] = useState<IContentMetadata | undefined>(undefined);

  const handleContentClick = () => {
    setSelectedContent(data.content_metadata);
    setDrawerOpen(true);
  };

  const handleCloseDrawer = () => {
    setDrawerOpen(false);
  };

  return (
    <>
      <Paper
        sx={{
          paddingTop: 2,
          paddingBottom: 2,
          marginBottom: 2,
          boxShadow: "0px 0px 2px 0px #D0D6DE",
          display: "flex",
          flexDirection: "column",
          borderRadius: "12px"
        }}
      >
        <Box display={"flex"} flexDirection={"column"} ml={2} mr={2} mb={2}>
          <Typography variant="overline">CONTENT ID: </Typography>
          <Typography
            onClick={handleContentClick}
            style={{ 
              color: "#1C2026", 
              fontWeight: 500, 
              cursor: "pointer",
              textDecoration: "underline"
            }}
          >
            {data.content_id}
          </Typography>
        </Box>
        <Divider variant="fullWidth" />
        <Box
          display={"flex"}
          flexDirection={"row"}
          alignItems={"center"}
          mt={2}
          ml={2}
          mr={2}
        >
          <DisplayData label="Confidence Score" value={data.confidence_score} />
          {Object.keys(data.labels).length !== 0 &&
            <Box display={"flex"} gap={1} ml={4} alignItems={"center"}>
              {Object.keys(data.labels).map((val: string) => {
                return (
                  <Chip
                    key={val}
                    label={`${val}:${data.labels[val]}`}
                    sx={{ backgroundColor: "#E5EFFB", color: "#1C2026" }}
                  />
                );
              })}
            </Box>}
        </Box>
      </Paper>
      <ContentDrawer
        open={drawerOpen}
        onClose={handleCloseDrawer}
        content={selectedContent}
        client={client}
      />
    </>
  );
};

export default SearchResultCard;

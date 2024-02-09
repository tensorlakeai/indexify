import * as React from "react";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import DataObjectIcon from "@mui/icons-material/DataObject";
import MemoryIcon from "@mui/icons-material/Memory";

export const MainListItems = ({
  currentNamespace,
}: {
  currentNamespace: string;
}) => {
  return (
    <React.Fragment>
      <ListItemButton href={`/ui/${currentNamespace}`}>
        <ListItemIcon>
          <DataObjectIcon />
        </ListItemIcon>
        <ListItemText primary={"Namespace"} />
      </ListItemButton>
      <ListItemButton href={`/ui/${currentNamespace}/extractors`}>
        <ListItemIcon>
          <MemoryIcon />
        </ListItemIcon>
        <ListItemText primary="Extractors" />
      </ListItemButton>
    </React.Fragment>
  );
};

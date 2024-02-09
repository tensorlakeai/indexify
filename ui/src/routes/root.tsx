import * as React from "react";
import { styled, ThemeProvider } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import MuiDrawer from "@mui/material/Drawer";
import Box from "@mui/material/Box";
import MuiAppBar, { AppBarProps as MuiAppBarProps } from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import List from "@mui/material/List";
import Typography from "@mui/material/Typography";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import Container from "@mui/material/Container";
import Link from "@mui/material/Link";
import MenuIcon from "@mui/icons-material/Menu";
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import { MainListItems } from "../components/listItems";
import {
  LoaderFunctionArgs,
  Outlet,
  redirect,
  useLoaderData,
} from "react-router-dom";
import theme from "../theme";
import { Stack } from "@mui/system";
import MenuItem from "@mui/material/MenuItem";
import Menu from "@mui/material/Menu";
import { Button } from "@mui/material";
import IndexifyClient from "../lib/Indexify/client";
import DataObjectIcon from "@mui/icons-material/DataObject";
import CircleIcon from "@mui/icons-material/Circle";
import { stringToColor } from "../utils/helpers";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespaces = (await IndexifyClient.namespaces()).map(
    (repo) => repo.name
  );

  if (!params.namespace || !namespaces.includes(params.namespace)) {
    if (params.namespace !== "default") {
      return redirect(`/${namespaces[0] ?? "default"}`);
    }
  }
  return { namespaces, namespace: params.namespace };
}

function Copyright(props: any) {
  return (
    <Typography
      variant="body2"
      color="text.secondary"
      align="center"
      {...props}
    >
      {"Copyright © "}
      <Link color="inherit" href="https://tensorlake.ai/">
        Tensorlake
      </Link>{" "}
      {new Date().getFullYear()}
      {"."}
    </Typography>
  );
}

const drawerWidth: number = 240;

interface AppBarProps extends MuiAppBarProps {
  open?: boolean;
}

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== "open",
})<AppBarProps>(({ theme, open }) => ({
  zIndex: theme.zIndex.drawer + 1,
  transition: theme.transitions.create(["width", "margin"], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  ...(open && {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(["width", "margin"], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  }),
}));

const Drawer = styled(MuiDrawer, {
  shouldForwardProp: (prop) => prop !== "open",
})(({ theme, open }) => ({
  "& .MuiDrawer-paper": {
    position: "relative",
    whiteSpace: "nowrap",
    width: drawerWidth,
    transition: theme.transitions.create("width", {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
    boxSizing: "border-box",
    ...(!open && {
      overflowX: "hidden",
      transition: theme.transitions.create("width", {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
      }),
      width: theme.spacing(7),
      [theme.breakpoints.up("sm")]: {
        width: theme.spacing(9),
      },
    }),
  },
}));

export default function Dashboard() {
  const { namespace, namespaces } = useLoaderData() as {
    namespace: string;
    namespaces: string[];
  };
  const [anchorEl, setAnchorEl] = React.useState<null | HTMLElement>(null);

  const handleMenu = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const [open, setOpen] = React.useState(true);
  const toggleDrawer = () => {
    setOpen(!open);
  };

  return (
    <ThemeProvider theme={theme}>
      <Box sx={{ display: "flex" }}>
        <CssBaseline />
        <AppBar
          position="absolute"
          open={open}
          sx={{ backgroundColor: "white" }}
        >
          <Toolbar
            sx={{
              pr: "24px", // keep right padding when drawer closed
            }}
          >
            <IconButton
              edge="start"
              color="inherit"
              aria-label="open drawer"
              onClick={toggleDrawer}
              sx={{
                marginRight: "36px",
                ...(open && { display: "none" }),
              }}
            >
              <MenuIcon sx={{ color: "#060D3F" }} />
            </IconButton>
            <Stack
              direction={"row"}
              display={"flex"}
              alignItems={"center"}
              justifyContent={"flex-start"}
              spacing={2}
              flexGrow={1}
            >
              <img src="/logo.svg" alt="logo" />
              <a
                href={"/ui"}
                style={{ textDecoration: "none", color: "white" }}
              >
                <Typography
                  component="h1"
                  variant="h6"
                  color="#060D3F"
                  noWrap
                  sx={{ flexGrow: 1 }}
                >
                  Indexify
                </Typography>
              </a>
            </Stack>

            <Button
              size="large"
              aria-label="account of current user"
              aria-controls="menu-appbar"
              aria-haspopup="true"
              onClick={handleMenu}
              startIcon={
                <DataObjectIcon sx={{ color: stringToColor(namespace) }} />
              }
            >
              {namespace}
            </Button>

            <Menu
              id="menu-appbar"
              anchorEl={anchorEl}
              anchorOrigin={{
                vertical: "top",
                horizontal: "right",
              }}
              keepMounted
              transformOrigin={{
                vertical: "top",
                horizontal: "right",
              }}
              open={Boolean(anchorEl)}
              onClose={handleClose}
            >
              <Typography mx={2} my={1} variant="h4">
                Namespaces ({namespaces.length})
              </Typography>
              {namespaces.map((name) => {
                return (
                  <a
                    style={{ textDecoration: "none" }}
                    key={name}
                    href={`/ui/${name}`}
                  >
                    <MenuItem onClick={handleClose}>
                      <CircleIcon
                        sx={{
                          width: "15px",
                          color: stringToColor(name),
                          mr: 1,
                        }}
                      />
                      {name}
                    </MenuItem>
                  </a>
                );
              })}
            </Menu>
          </Toolbar>
        </AppBar>
        <Drawer variant="permanent" open={open}>
          <Toolbar
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "flex-end",
              px: [1],
            }}
          >
            <IconButton onClick={toggleDrawer}>
              <ChevronLeftIcon />
            </IconButton>
          </Toolbar>
          <Divider />
          <List component="nav">
            <MainListItems currentNamespace={namespace} />
            {/* <Divider sx={{ my: 1 }} />
            {secondaryListItems} */}
          </List>
        </Drawer>
        <Box
          component="main"
          sx={{
            backgroundColor: (theme) =>
              theme.palette.mode === "light"
                ? theme.palette.grey[100]
                : theme.palette.grey[900],
            flexGrow: 1,
            height: "100vh",
            overflow: "auto",
          }}
        >
          <Toolbar />
          <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
            <div id="detail">
              <Outlet />
            </div>
            <Copyright sx={{ pt: 4 }} />
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

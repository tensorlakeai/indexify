import { ThemeProvider } from '@mui/material/styles'
import CssBaseline from '@mui/material/CssBaseline'
import Box from '@mui/material/Box'
import Toolbar from '@mui/material/Toolbar'
import Typography from '@mui/material/Typography'
import {
  LoaderFunctionArgs,
  Outlet,
  redirect,
  useLoaderData,
  useLocation,
} from 'react-router-dom'
import theme from '../theme'
import { Stack } from '@mui/system'
import { IndexifyClient } from 'getindexify'
import { getIndexifyServiceURL } from '../utils/helpers'
import Footer from '../components/Footer'
import {
  Divider,
  Drawer,
  List,
  ListItemButton,
  ListItemText,
} from '@mui/material'
import { Link } from 'react-router-dom'
import HistoryIcon from '@mui/icons-material/History';
import { Cpu, Data, Grid7, MobileProgramming } from 'iconsax-react'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespaces = (
    await IndexifyClient.namespaces({
      serviceUrl: getIndexifyServiceURL(),
    })
  ).map((repo) => repo.name)

  if (!params.namespace || !namespaces.includes(params.namespace)) {
    if (params.namespace !== 'default') {
      return redirect(`/${namespaces[0] ?? 'default'}/extraction-graphs`)
    }
  }
  return { namespaces, namespace: params.namespace }
}

const drawerWidth = 240

export default function Dashboard() {
  const { namespace } = useLoaderData() as {
    namespace: string
    namespaces: string[]
  }
  const location = useLocation()
  return (
    <ThemeProvider theme={theme}>
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          height: '100vh',
          backgroundColor: (theme) =>
            theme.palette.mode === 'light'
              ? '#F7F9FC'
              : theme.palette.grey[900],
        }}
      >
        <CssBaseline />
        <Drawer
          variant="permanent"
          sx={{
            width: drawerWidth,
            flexShrink: 0,
            [`& .MuiDrawer-paper`]: {
              width: drawerWidth,
              boxSizing: 'border-box',
            },
          }}
        >
          <Toolbar>
            <Stack
              direction={'row'}
              display={'flex'}
              alignItems={'center'}
              justifyContent={'flex-start'}
              spacing={2}
            >
              <img src="/ui/logo.svg" alt="logo" />
              <a
                href={'/ui'}
                style={{ textDecoration: 'none', color: 'white' }}
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
          </Toolbar>
          <Box
            sx={{
              overflow: 'auto',
            }}
          >
            <List>
              <ListItemButton
                to={`/${namespace}/extractors`}
                component={Link}
                selected={location.pathname.startsWith(`/${namespace}/extractors`)}
                className={location.pathname.startsWith(`/${namespace}/extractors`) ? "selected-navbar-items navbar-items" : "navbar-items"}
              >
                <Data size="20" className="drawer-logo" variant="Outline" />
                <ListItemText primary={'Extractors'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/extraction-graphs`}
                component={Link}
                selected={
                  location.pathname.startsWith(`/${namespace}/extraction-graphs`)
                }
                className={location.pathname.startsWith(`/${namespace}/extraction-graphs`) ? "selected-navbar-items navbar-items" : "navbar-items"}
              >
                <Cpu size="20" className="drawer-logo" variant="Outline" />
                <ListItemText primary={'Extraction Graphs'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/indexes`}
                component={Link}
                selected={location.pathname.startsWith(`/${namespace}/indexes`)}
                className={location.pathname.startsWith(`/${namespace}/indexes`) ? "selected-navbar-items navbar-items" : "navbar-items"}
              >
                <MobileProgramming size="20" className="drawer-logo" variant="Outline"/>
                <ListItemText primary={'Indexes'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/sql-tables`}
                component={Link}
                selected={location.pathname.startsWith(`/${namespace}/sql-tables`)}
                className={location.pathname.startsWith(`/${namespace}/sql-tables`) ? "selected-navbar-items navbar-items" : "navbar-items"}
              >
                <Grid7 size="20" className="drawer-logo" variant="Outline"/>
                <ListItemText primary={'SQL Tables'} />
              </ListItemButton>
            <ListItemButton
                to={`/${namespace}/state-changes`}
                component={Link}
                selected={location.pathname.startsWith(`/${namespace}/state-changes`)}
                className={location.pathname.startsWith(`/${namespace}/state-changes`) ? "selected-navbar-items navbar-items" : "navbar-items"}
              >
                <HistoryIcon className="drawer-logo" />
                <ListItemText primary={'System Events'} />
              </ListItemButton>
            </List>
          </Box>
        </Drawer>
        {/* page content */}
        <Box
          component="main"
          display="flex"
          flexDirection={'column'}
          sx={{
            minHeight: '100vh',
            overflow: 'auto',
            padding: 2,
            marginLeft: `${drawerWidth}px`,
          }}
        >
          <Box id="detail" p={2} flexGrow={1}>
            <Outlet />
          </Box>
          <Divider />
          <Footer />
        </Box>
      </Box>
    </ThemeProvider>
  )
}

import * as React from 'react'
import { styled, ThemeProvider } from '@mui/material/styles'
import CssBaseline from '@mui/material/CssBaseline'
import Box from '@mui/material/Box'
import MuiAppBar, { AppBarProps as MuiAppBarProps } from '@mui/material/AppBar'
import Toolbar from '@mui/material/Toolbar'
import Typography from '@mui/material/Typography'
import Container from '@mui/material/Container'
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
import { getIndexifyServiceURL, stringToColor } from '../utils/helpers'
import Footer from '../components/Footer'
import NavigationBar from '../components/Navigation/NavigationBar'
import { Drawer, List, ListItemButton, ListItemText } from '@mui/material'
import { Link } from 'react-router-dom'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespaces = (
    await IndexifyClient.namespaces({
      serviceUrl: getIndexifyServiceURL(),
    })
  ).map((repo) => repo.name)

  if (!params.namespace || !namespaces.includes(params.namespace)) {
    if (params.namespace !== 'default') {
      return redirect(`/${namespaces[0] ?? 'default'}`)
    }
  }
  return { namespaces, namespace: params.namespace }
}

const drawerWidth = 240

export default function Dashboard() {
  const { namespace, namespaces } = useLoaderData() as {
    namespace: string
    namespaces: string[]
  }
  const location = useLocation()
  return (
    <ThemeProvider theme={theme}>
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
        <CssBaseline />
        {/* <NavigationBar namespace={namespace} namespaces={namespaces} /> */}
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
          <Box sx={{ overflow: 'auto' }}>
            <List>
              <ListItemButton
                to={`/${namespace}/extractors`}
                component={Link}
                selected={location.pathname === `/${namespace}/extractors`}
              >
                <ListItemText primary={'Extractors'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/content`}
                component={Link}
                selected={location.pathname === `/${namespace}/content`}
              >
                <ListItemText primary={'Content'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/extraction-graphs`}
                component={Link}
                selected={
                  location.pathname === `/${namespace}/extraction-graphs`
                }
              >
                <ListItemText primary={'Extraction Graphs'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/indexes`}
                component={Link}
                selected={location.pathname === `/${namespace}/indexes`}
              >
                <ListItemText primary={'Indexes'} />
              </ListItemButton>
              <ListItemButton
                to={`/${namespace}/sql-tables`}
                component={Link}
                selected={location.pathname === `/${namespace}/sql-tables`}
              >
                <ListItemText primary={'SQL Tables'} />
              </ListItemButton>
            </List>
          </Box>
        </Drawer>
        {/* page content */}
        <Box
          component="main"
          sx={{
            backgroundColor: (theme) =>
              theme.palette.mode === 'light'
                ? theme.palette.grey[100]
                : theme.palette.grey[900],
            flexGrow: 1,
            overflow: 'auto',
            padding: 2,
            marginLeft: `${drawerWidth}px`,
          }}
        >
          <Container maxWidth="lg">
            <Box id="detail">
              <Outlet />
            </Box>
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  )
}

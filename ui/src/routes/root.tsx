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
} from 'react-router-dom'
import theme from '../theme'
import { Stack } from '@mui/system'
import { IndexifyClient } from 'getindexify'
import { getIndexifyServiceURL, stringToColor } from '../utils/helpers'
import Footer from '../components/Footer'
import NamespaceDropdown from '../components/Navigation/NamespaceDropdown'
import NavigationBar from '../components/Navigation/NavigationBar'
import NavigationDrawer from '../components/Navigation/NavigationDrawer'
import {
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
} from '@mui/material'

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

interface AppBarProps extends MuiAppBarProps {
  open?: boolean
}

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== 'open',
})<AppBarProps>(({ theme, open }) => ({
  transition: theme.transitions.create(['width', 'margin'], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
}))

const drawerWidth = 240

export default function Dashboard() {
  const { namespace, namespaces } = useLoaderData() as {
    namespace: string
    namespaces: string[]
  }
  return (
    <ThemeProvider theme={theme}>
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
        <CssBaseline />
        <NavigationBar namespace={namespace} namespaces={namespaces} />
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
              <ListItemButton>
                <ListItemText primary={'Extractors'} />
              </ListItemButton>
              <ListItemButton>
                <ListItemText primary={'Extraction Graphs'} />
              </ListItemButton>
              <ListItemButton>
                <ListItemText primary={'Indexes'} />
              </ListItemButton>
              <ListItemButton>
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
            <div id="detail">
              <Outlet />
            </div>
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  )
}

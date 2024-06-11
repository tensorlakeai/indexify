import { MenuItem, Typography } from '@mui/material'
import CircleIcon from '@mui/icons-material/Circle'
import DataObjectIcon from '@mui/icons-material/DataObject'
import { Button } from '@mui/material'
import Menu from '@mui/material/Menu'
import { useState } from 'react'
import { stringToColor } from '../../utils/helpers'

interface Props {
  namespace: string
  namespaces: string[]
}
const NamespaceDropdown = ({ namespaces, namespace }: Props) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)

  const handleMenu = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget)
  }

  const handleClose = () => {
    setAnchorEl(null)
  }

  return (
    <>
      <Button
        size="large"
        aria-label="account of current user"
        aria-controls="menu-appbar"
        aria-haspopup="true"
        onClick={handleMenu}
        startIcon={<DataObjectIcon sx={{ color: stringToColor(namespace) }} />}
      >
        {namespace}
      </Button>

      <Menu
        id="menu-appbar"
        anchorEl={anchorEl}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        keepMounted
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
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
              style={{ textDecoration: 'none' }}
              key={name}
              href={`/ui/${name}`}
            >
              <MenuItem onClick={handleClose}>
                <CircleIcon
                  sx={{
                    width: '15px',
                    color: stringToColor(name),
                    mr: 1,
                  }}
                />
                {name}
              </MenuItem>
            </a>
          )
        })}
      </Menu>
    </>
  )
}

export default NamespaceDropdown

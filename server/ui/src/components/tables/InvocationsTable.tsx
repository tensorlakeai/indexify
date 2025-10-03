// import {
//   Table,
//   TableBody,
//   TableCell,
//   TableContainer,
//   TableHead,
//   TableRow,
//   Paper,
//   Typography,
//   Box,
//   IconButton,
// } from '@mui/material'
// import DeleteIcon from '@mui/icons-material/Delete'
// import axios from 'axios'
// import { Link } from 'react-router-dom'
// import CopyText from '../CopyText'
// import { formatTimestamp, getIndexifyServiceURL } from '../../utils/helpers'
// import { ShallowGraphRequest } from '../../types/types'

// interface InvocationsTableProps {
//   invocationsList: ShallowGraphRequest[]
//   computeGraph: string
//   namespace: string
//   onDelete: (updatedList: ShallowGraphRequest[]) => void
// }

// export function InvocationsTable({
//   invocationsList,
//   computeGraph,
//   onDelete,
//   namespace,
// }: InvocationsTableProps) {
//   const handleDelete = async (invocationId: string) => {
//     try {
//       await axios.delete(
//         `${getIndexifyServiceURL()}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}`,
//         { headers: { accept: '*/*' } }
//       )

//       onDelete(
//         invocationsList.filter((invocation) => invocation.id !== invocationId)
//       )
//     } catch (error) {
//       console.error(`Error deleting invocation ${invocationId}:`, error)
//     }
//   }

//   return (
//     <Box sx={{ width: '100%', mt: 2 }}>
//       <Typography variant="h6" gutterBottom>
//         Invocations
//       </Typography>
//       <TableContainer
//         component={Paper}
//         sx={{ boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset' }}
//       >
//         <Table>
//           <TableHead>
//             <TableRow>
//               <TableCell>ID</TableCell>
//               <TableCell>Created At</TableCell>
//               <TableCell>Outcome</TableCell>
//               <TableCell>Status</TableCell>
//               <TableCell>Action</TableCell>
//             </TableRow>
//           </TableHead>
//           <TableBody>
//             {invocationsList.map(({ id, created_at, status, outcome }) => (
//               <TableRow key={id}>
//                 <TableCell>
//                   <Box display="flex" flexDirection="row" alignItems="center">
//                     <Link
//                       to={`/${namespace}/compute-graphs/${computeGraph}/invocations/${id}`}
//                     >
//                       {id}
//                     </Link>
//                     <CopyText text={id} />
//                   </Box>
//                 </TableCell>
//                 <TableCell>{formatTimestamp(created_at)}</TableCell>
//                 <TableCell>{outcome}</TableCell>
//                 <TableCell>{status}</TableCell>
//                 <TableCell>
//                   <IconButton
//                     onClick={() => handleDelete(id)}
//                     color="error"
//                     size="small"
//                   >
//                     <DeleteIcon />
//                   </IconButton>
//                 </TableCell>
//               </TableRow>
//             ))}
//           </TableBody>
//         </Table>
//       </TableContainer>
//     </Box>
//   )
// }

// export default InvocationsTable

export {}

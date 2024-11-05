import { Suspense } from 'react'
import { Breadcrumbs, Typography } from '@mui/material'
import { TableDocument } from 'iconsax-react'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData } from 'react-router-dom'
import CopyText from '../../components/CopyText'
import InvocationTasksTable from '../../components/tables/InvocationTasksTable'
import InvocationOutputTable from '../../components/tables/InvocationOutputTable'


interface InvocationPageData {
  indexifyServiceURL: string
  invocationId: string
  computeGraph: string
  namespace: string
}

function BreadcrumbTrail({ namespace, computeGraph, invocationId }: Omit<InvocationPageData, 'indexifyServiceURL'>) {
  return (
    <Breadcrumbs
      aria-label="invocation navigation"
      separator={<NavigateNextIcon fontSize="small" />}
    >
      <Typography color="text.primary">{namespace}</Typography>
      <Link to={`/${namespace}/compute-graphs`}>
        <Typography color="text.primary">Compute Graphs</Typography>
      </Link>
      <Link to={`/${namespace}/compute-graphs/${computeGraph}`}>
        <Typography color="text.primary">{computeGraph}</Typography>
      </Link>
      <Typography color="text.primary">{invocationId}</Typography>
    </Breadcrumbs>
  )
}

function InvocationHeader({ invocationId }: { invocationId: string }) {
  return (
    <div className="flex items-center gap-2">
      <div className="flex items-center justify-center w-8 h-8">
        <TableDocument size={25} className="text-primary" variant="Outline"/>
      </div>
      <Typography variant="h4" className="flex flex-row items-center gap-2">
        Invocation - {invocationId} 
        <CopyText text={invocationId} />
      </Typography>
    </div>
  )
}

function IndividualInvocationPage() {
  const { indexifyServiceURL, invocationId, computeGraph, namespace } = useLoaderData() as InvocationPageData

  return (
    <div className="flex flex-col gap-6">
      <BreadcrumbTrail
        namespace={namespace}
        computeGraph={computeGraph}
        invocationId={invocationId}
      />
      
      <div className="space-y-6">
        <InvocationHeader invocationId={invocationId} />
        
        <Suspense fallback={<div>Loading output table...</div>}>
          <InvocationOutputTable
            indexifyServiceURL={indexifyServiceURL}
            invocationId={invocationId}
            namespace={namespace}
            computeGraph={computeGraph}
          />
        </Suspense>

        <Suspense fallback={<div>Loading tasks table...</div>}>
          <InvocationTasksTable
            indexifyServiceURL={indexifyServiceURL}
            invocationId={invocationId}
            namespace={namespace}
            computeGraph={computeGraph}
          />
        </Suspense>
      </div>
    </div>
  )
}

export default IndividualInvocationPage;

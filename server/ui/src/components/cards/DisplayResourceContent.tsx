import { HostResources } from '../../types'
import { bytesToGigabytes } from '../../utils/helpers'

interface DisplayResourceContentProps {
  resourceValue: HostResources
}

export default function DisplayResourceContent({
  resourceValue,
}: DisplayResourceContentProps) {
  return (
    <>
      <p>
        <strong>CPU Count:</strong> {resourceValue.cpu_count}
      </p>
      <p>
        <strong>Memory GB:</strong>{' '}
        {bytesToGigabytes(resourceValue.memory_bytes)}
      </p>
      <p>
        <strong>Disk GB:</strong> {bytesToGigabytes(resourceValue.disk_bytes)}
      </p>
      <p>
        <strong>GPU Information:</strong>
        <br />
        <span style={{ fontSize: '0.85rem', marginLeft: '1rem' }}>
          <strong style={{ color: '#444444' }}>Count:</strong>{' '}
          {resourceValue.gpu?.count ? resourceValue.gpu.count : 'N/A'}
        </span>
        <br />
        <span style={{ fontSize: '0.85rem', marginLeft: '1rem' }}>
          <strong style={{ color: '#444444' }}>Model:</strong>{' '}
          {resourceValue.gpu?.model ? resourceValue.gpu.model : 'N/A'}
        </span>
      </p>
    </>
  )
}

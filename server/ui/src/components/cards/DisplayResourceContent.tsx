import { GPUResources } from '../../types'

interface DisplayResourceContentProps {
  keyName: string
  value: string | number | GPUResources | null
}

export default function DisplayResourceContent({
  keyName,
  value,
}: DisplayResourceContentProps) {
  if (typeof value === 'object') {
    return (
      <p>
        <strong>{keyName}:</strong>
        {value?.count !== undefined ? (
          <>
            <br />
            <span style={{ fontSize: '0.9rem', marginLeft: '1rem' }}>
              <strong style={{ color: '#444444' }}>count:</strong> {value.count}
            </span>
            <br />
            <span style={{ fontSize: '0.9rem', marginLeft: '1rem' }}>
              <strong style={{ color: '#444444' }}>model:</strong> {value.model}
            </span>
          </>
        ) : (
          ' -'
        )}
      </p>
    )
  }

  return (
    <p>
      <strong>{keyName}:</strong>{' '}
      {value === null ? 'null' : value ? value : '-'}
    </p>
  )
}

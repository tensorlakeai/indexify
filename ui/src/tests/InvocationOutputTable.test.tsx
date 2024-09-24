import { render, screen, waitFor } from '@testing-library/react';
import axios from 'axios';
import InvocationOutputTable from '../components/tables/InvocationOutputTable';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

const mockOutputs = {
  "outputs": [
    {
      "compute_fn": "extractor_a",
      "id": "23b534e020ba7756"
    },
    {
      "compute_fn": "extractor_b",
      "id": "5d2dd3c089a04bdc"
    },
    {
      "compute_fn": "extractor_b",
      "id": "e91dd8519b99e157"
    },
    {
      "compute_fn": "extractor_c",
      "id": "150c8ca97ba0aebd"
    },
    {
      "compute_fn": "extractor_c",
      "id": "7d85eeae3a043312"
    }
  ]
};

describe('OutputsTable', () => {
  beforeEach(() => {
    mockedAxios.get.mockReset();
  });

  it('renders outputs table with correct data', async () => {
    mockedAxios.get.mockResolvedValue({ data: mockOutputs });

    render(<InvocationOutputTable invocationId="83cc4443d738b61a" namespace="default" computeGraph="graph_a" />);

    await waitFor(() => {
      expect(screen.getByText('Outputs for Invocation 83cc4443d738b61a')).toBeInTheDocument();
    });

    expect(screen.getByText('Compute Function')).toBeInTheDocument();
    expect(screen.getByText('ID')).toBeInTheDocument();

    mockOutputs.outputs.forEach(output => {
      expect(screen.getByText(output.compute_fn)).toBeInTheDocument();
      expect(screen.getByText(output.id)).toBeInTheDocument();
    });
  });

  it('handles API error', async () => {
    mockedAxios.get.mockRejectedValue(new Error('API Error'));

    render(<InvocationOutputTable invocationId="83cc4443d738b61a" namespace="default" computeGraph="graph_a" />);

    await waitFor(() => {
      expect(screen.getByText('Outputs for Invocation 83cc4443d738b61a')).toBeInTheDocument();
    });

    expect(screen.queryByText('23b534e020ba7756')).not.toBeInTheDocument();
  });
});
import axios, { AxiosInstance, AxiosResponse } from "axios";
import Repository from "./repository";

const DEFAULT_SERVICE_URL = "http://localhost:8900"; // Set your default service URL

class IndexifyClient {
  private serviceUrl: string;
  private client: AxiosInstance;

  constructor(serviceUrl: string = DEFAULT_SERVICE_URL) {
    this.serviceUrl = serviceUrl;
    this.client = axios.create();
  }

  private async request(
    method: string,
    endpoint: string,
    options: any = {}
  ): Promise<AxiosResponse> {
    try {
      const response = await this.client.request({
        method,
        url: `${this.serviceUrl}/${endpoint}`,
        ...options,
      });
      return response;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error(`Error: ${error.message}`);
      }
      throw error;
    }
  }

  async get(endpoint: string): Promise<AxiosResponse> {
    return this.request("GET", endpoint);
  }

  async post(endpoint: string): Promise<AxiosResponse> {
    return this.request("POST", endpoint);
  }

  async repositories(): Promise<Repository[]> {
    const response = await this.get("repositories");
    const repositoriesData = response.data.repositories as any[];
    return repositoriesData.map(
      (rd) => new Repository(rd.name, this.serviceUrl)
    );
  }
}

export default IndexifyClient;

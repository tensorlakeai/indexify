import axios from "axios";
import { IContent, IIndex } from "./types";
class Repository {
  private serviceUrl: string;
  public name: string;

  constructor(serviceUrl: string, name: string) {
    this.serviceUrl = serviceUrl;
    this.name = name;
  }

  async indexes(): Promise<IIndex[]> {
    const resp = await axios.get(
      `${this.serviceUrl}/repositories/${this.name}/indexes`
    );
    return resp.data.indexes;
  }

  async getContent(): Promise<IContent[]> {
    const resp = await axios.get(
      `${this.serviceUrl}/repositories/${this.name}/content`
    );
    return resp.data.content_list;
  }
}

export default Repository;

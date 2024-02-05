import axios from "axios";
import { IContent, IExtractorBinding, IIndex } from "./types";
class Repository {
  private serviceUrl: string;
  public name: string;
  public extractorBindings: IExtractorBinding[];
  public filters: Record<string, string>;

  constructor(
    serviceUrl: string,
    name: string,
    extractorBindings: IExtractorBinding[] = [],
    filters: Record<string, string> = {}
  ) {
    this.serviceUrl = serviceUrl;
    this.name = name;
    this.extractorBindings = extractorBindings;
    this.filters = filters;
  }

  async indexes(): Promise<IIndex[]> {
    const resp = await axios.get(
      `${this.serviceUrl}/repositories/${this.name}/indexes`
    );
    return resp.data.indexes;
  }

  async getContent(
    parent_id?: string,
    labels_eq?: string
  ): Promise<IContent[]> {
    const resp = await axios.get(
      `${this.serviceUrl}/repositories/${this.name}/content`,
      {
        params: { parent_id, labels_eq },
      }
    );
    return resp.data.content_list;
  }
}

export default Repository;

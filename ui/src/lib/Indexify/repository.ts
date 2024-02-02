class Repository {
  private serviceUrl: string;
  public name: string;

  constructor(serviceUrl: string, name: string) {
    this.serviceUrl = serviceUrl;
    this.name = name;
  }
}

export default Repository;

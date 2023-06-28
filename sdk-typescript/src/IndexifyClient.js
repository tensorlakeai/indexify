"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.IndexifyClient = void 0;
const AxiosHttpRequest_1 = require("./core/AxiosHttpRequest");
const IndexifyService_1 = require("./services/IndexifyService");
class IndexifyClient {
    indexify;
    request;
    constructor(config, HttpRequest = AxiosHttpRequest_1.AxiosHttpRequest) {
        this.request = new HttpRequest({
            BASE: config?.BASE ?? '',
            VERSION: config?.VERSION ?? '0.1.0',
            WITH_CREDENTIALS: config?.WITH_CREDENTIALS ?? false,
            CREDENTIALS: config?.CREDENTIALS ?? 'include',
            TOKEN: config?.TOKEN,
            USERNAME: config?.USERNAME,
            PASSWORD: config?.PASSWORD,
            HEADERS: config?.HEADERS,
            ENCODE_PATH: config?.ENCODE_PATH,
        });
        this.indexify = new IndexifyService_1.IndexifyService(this.request);
    }
}
exports.IndexifyClient = IndexifyClient;

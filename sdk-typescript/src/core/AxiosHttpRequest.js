"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AxiosHttpRequest = void 0;
const BaseHttpRequest_1 = require("./BaseHttpRequest");
const request_1 = require("./request");
class AxiosHttpRequest extends BaseHttpRequest_1.BaseHttpRequest {
    constructor(config) {
        super(config);
    }
    /**
     * Request method
     * @param options The request options from the service
     * @returns CancelablePromise<T>
     * @throws ApiError
     */
    request(options) {
        return (0, request_1.request)(this.config, options);
    }
}
exports.AxiosHttpRequest = AxiosHttpRequest;

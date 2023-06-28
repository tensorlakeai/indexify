"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const OpenAPI_1 = require("../src/core/OpenAPI");
const IndexifyService_1 = require("../src/services/IndexifyService");
OpenAPI_1.OpenAPI.BASE = 'http://localhost:8900';
async function main() {
    var resp = await IndexifyService_1.IndexifyService.addTexts({
        "documents": [
            {
                "text": "hello world",
                "metadata": { "key": "k3" }
            },
            {
                "text": "Indexify is amazing!",
                "metadata": { "key": "k4" }
            }
        ]
    });
    console.log(resp);
    var searchResp = await IndexifyService_1.IndexifyService.indexSearch({
        index: "default/default",
        k: 2,
        query: "hello"
    });
    console.log(searchResp);
}
main();

"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const IndexifyClient_1 = require("../src/IndexifyClient");
const indexifyClient = new IndexifyClient_1.IndexifyClient({
    BASE: "http://localhost:8900",
});
async function main() {
    var resp = await indexifyClient.indexify.addTexts({
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
    var searchResp = await indexifyClient.indexify.indexSearch({
        index: "default/default",
        k: 2,
        query: "hello"
    });
    console.log(searchResp);
}
main();

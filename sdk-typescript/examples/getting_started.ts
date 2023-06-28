import { OpenAPI } from '../src/core/OpenAPI';

import { IndexifyService } from '../src/services/IndexifyService';

OpenAPI.BASE = 'http://localhost:8900';

async function main() {
    var resp = await IndexifyService.addTexts({
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


    var searchResp = await IndexifyService.indexSearch({
        index: "default/default",
        k: 2,
        query: "hello"
    });

    console.log(searchResp);
}

main();

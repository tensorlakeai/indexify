const {IndexifyClient} = require('../src/IndexifyClient');
const {IndexifyService} = require('../src/services/IndexifyService');

const indexifyClient = new IndexifyClient({
    BASE: "http://localhost:8900",
});

async function main() {
    var resp = await indexifyClient.indexify.addTexts({
        "documents": [
            {
                "text": "hello world",
                "metadata": {"key": "k3"}
            },
            {
                "text": "Indexify is amazing!",
                "metadata": {"key": "k4"}
            }
        ]
    });
    console.log(resp);


    var searchResp = await indexifyClient.indexify.indexSearch({
        repository: "default",
        index: "default_index",
        query: "good",
        k: 1,
    });

    console.log(searchResp);
}

main();

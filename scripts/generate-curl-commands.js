const { Octokit } = require("@octokit/core");
const fs = require("fs");

const octokit = new Octokit({
    auth: process.env.GITHUB_PERSONAL_ACCESS_TOKEN
});

async function generateCurlCommands(owner, repo) {
    try {
        const response = await octokit.request('GET /repos/{owner}/{repo}/releases/latest', {
            owner,
            repo
        });

        const { tag_name, assets } = response.data;

        const zipUrl = `https://github.com/${owner}/${repo}/archive/refs/tags/${tag_name}.zip`;
        const tarGzUrl = `https://github.com/${owner}/${repo}/archive/refs/tags/${tag_name}.tar.gz`;

        const darwinAssets = assets.filter(asset => asset.name.includes('darwin'));
        const linuxAssets = assets.filter(asset => asset.name.includes('linux'));
        const windowsAssets = assets.filter(asset => asset.name.includes('windows'));

        const darwinCommands = darwinAssets.map(asset =>
            `curl -L -o ${asset.name} "${asset.browser_download_url}"`);
        const linuxCommands = linuxAssets.map(asset =>
            `curl -L -o ${asset.name} "${asset.browser_download_url}"`);
        const windowsCommands = windowsAssets.map(asset =>
            `curl -L -o ${asset.name} "${asset.browser_download_url}"`);

        const darwinUrls = darwinAssets.map(asset => asset.browser_download_url);
        const linuxUrls = linuxAssets.map(asset => asset.browser_download_url);
        const windowsUrls = windowsAssets.map(asset => asset.browser_download_url);

        const result = {
            zipCommand: `curl -L -o ${repo}-${tag_name}.zip "${zipUrl}"`,
            tarGzCommand: `curl -L -o ${repo}-${tag_name}.tar.gz "${tarGzUrl}"`,
            darwinCommands,
            linuxCommands,
            windowsCommands,
            darwinUrls,
            linuxUrls,
            windowsUrls
        };
        fs.writeFileSync('release.json', JSON.stringify(result, null, 2));
    } catch (error) {
        console.error('Failed to fetch latest release:', error);
    }
}

const [owner, repo] = process.argv.slice(2);

generateCurlCommands(owner, repo);

export { generateCurlCommands };

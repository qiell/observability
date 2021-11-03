const child_process = require("child_process");
const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");

function getOptions() {
    const dashboardsBase = path.resolve(".", "../../microlith/grafana/provisioning/dashboards");
    if ("GITHUB_TOKEN" in process.env) {
        const context = require("@actions/github").context;
        if ("inputs" in context.payload) {
            let files;
            if (context.payload.inputs.files === "all") {
                files = fs.readdirSync(dashboardsBase).map(x => path.join(dashboardsBase, x)).filter(x => /\.json$/.test(x));
            } else {
                files = context.payload.inputs.files.split(/,\s?/).map(x => path.resolve(dashboardsBase, x + ".json"));
            }
            let pullRequest;
            if (context.payload.inputs.pr) {
                pullRequest = parseInt(context.payload.inputs.pr);
            }
            return {
                files,
                pullRequest
            };
        } else {
            const base = context.payload.pull_request.base.sha;
            const head = context.payload.pull_request.head.sha;
            const changedFiles = child_process.execSync(`git diff --name-only ${base} ${head}`, { encoding: "ascii" }).trim().split("\n");
            return {
                files: changedFiles,
                pullRequest: context.payload.pull_request.number
            };
        }
    } else {
        if (process.argv.length < 3) {
            throw new Error("Insufficient arguments. Usage: node index.js [comma-separated-files | 'all'");
        }
        const filesInput = process.argv[2];
        if (filesInput.trim() === "all") {
            files = fs.readdirSync(dashboardsBase).map(x => path.join(dashboardsBase, x)).filter(x => /\.json$/.test(x));
        } else {
            files = filesInput.split(/,\s?/).map(x => path.resolve(dashboardsBase, x + ".json"));
        }
        return { files, pullRequest: undefined };
    }
}

(async function(){
    const {files, pullRequest} = getOptions();
    console.log("Taking screenshots of", files);

    const browser = await puppeteer.launch({ headless: true });
    const screenshots = [];

    await Promise.all(files.map(async file => {
        const absolute = path.resolve(__dirname, "../..", file);
        const base = path.basename(file).replace(".json", "");
        const definition = require(absolute);
        const uid = definition.uid;

        const page = await browser.newPage();
        await page.setViewport({ width: 1440, height: 1080 });
        await page.goto(`http://localhost:8080/grafana/d/${uid}`);
        await page.waitForNetworkIdle();
        const ssPath = path.join(__dirname, `${base}.png`);
        await page.screenshot({ path: ssPath });
        screenshots.push(ssPath);
    }));

    await browser.close();

    console.log("Screenshots saved: " + screenshots);
})();
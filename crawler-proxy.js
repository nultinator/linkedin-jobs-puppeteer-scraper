const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;

async function writeToCsv(data, outputFile) {
    let success = false;
    while (!success) {

        if (!data || data.length === 0) {
            throw new Error("No data to write!");
        }
        const fileExists = fs.existsSync(outputFile);
    
        if (!(data instanceof Array)) {
            data = [data]
        }
    
        const headers = Object.keys(data[0]).map(key => ({id: key, title: key}))
    
        const csvWriter = createCsvWriter({
            path: outputFile,
            header: headers,
            append: fileExists
        });
        try {
            await csvWriter.writeRecords(data);
            success = true;
        } catch (e) {
            console.log("Failed data", data);
            throw new Error("Failed to write to csv");
        }
    }
}


function range(start, end) {
    const array = [];
    for (let i=start; i<end; i++) {
        array.push(i);
    }
    return array;
}

function getScrapeOpsUrl(url, location="us") {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url: url,
        country: location
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

async function scrapeSearchResults(browser, keyword, pageNumber, locality, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const formattedKeyword = keyword.replace(" ", "+");
        const formattedLocality = locality.replace(" ", "+");

        const page = await browser.newPage();
        try {
            const url = `https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=${formattedKeyword}&location=${formattedLocality}&original_referer=&start=${pageNumber*10}`;
    
            const proxyUrl = getScrapeOpsUrl(url, location);
            await page.goto(proxyUrl, { timeout: 0 });

            console.log(`Successfully fetched: ${url}`);

            const divCards = await page.$$("div[class='base-search-card__info']");

            for (const divCard of divCards) {

                const nameElement = await divCard.$("h4[class='base-search-card__subtitle']");
                const name = await page.evaluate(element => element.textContent, nameElement);

                const jobTitleElement = await divCard.$("h3[class='base-search-card__title']");
                const jobTitle = await page.evaluate(element => element.textContent, jobTitleElement);

                const parentElement = await page.evaluateHandle(element => element.parentElement, divCard);

                const aTag = await parentElement.$("a");
                const link = await page.evaluate(element => element.getAttribute("href"), aTag);

                const jobLocationElement = await divCard.$("span[class='job-search-card__location']");
                const jobLocation = await page.evaluate(element => element.textContent, jobLocationElement);

                const searchData = {
                    name: name.trim(),
                    job_title: jobTitle.trim(),
                    url: link.trim(),
                    location: jobLocation.trim()
                };

                await writeToCsv([searchData], `${keyword.replace(" ", "-")}.csv`);
            }

            success = true;

        } catch (err) {
            console.log(`Error: ${err}, tries left ${retries - tries}`);
            tries++;

        } finally {
            await page.close();
        } 
    }
}

async function startCrawl(keyword, pages, locality, location, concurrencyLimit, retries) {
    const pageList = range(0, pages);

    const browser = await puppeteer.launch();

    while (pageList.length > 0) {
        const currentBatch = pageList.splice(0, concurrencyLimit);
        const tasks = currentBatch.map(page => scrapeSearchResults(browser, keyword, page, locality, location, retries));

        try {
            await Promise.all(tasks);
        } catch (err) {
            console.log(`Failed to process batch: ${err}`);
        }
    }

    await browser.close();
}


async function main() {
    const keywords = ["software engineer"];
    const concurrencyLimit = 5;
    const pages = 3;
    const location = "us";
    const locality = "United States";
    const retries = 3;
    const aggregateFiles = [];

    for (const keyword of keywords) {
        console.log("Crawl starting");
        console.time("startCrawl");
        await startCrawl(keyword, pages, locality, location, concurrencyLimit, retries);
        console.timeEnd("startCrawl");
        console.log("Crawl complete");
        aggregateFiles.push(`${keyword.replace(" ", "-")}.csv`);
    }
}


main();
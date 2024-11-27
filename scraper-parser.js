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

async function readCsv(inputFile) {
    const results = [];
    const parser = fs.createReadStream(inputFile).pipe(csvParse.parse({
        columns: true,
        delimiter: ",",
        trim: true,
        skip_empty_lines: true
    }));

    for await (const record of parser) {
        results.push(record);
    }
    return results;
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

async function processJob(browser, row, location, retries = 3) {
    const url = row.url;
    let tries = 0;
    let success = false;

    
    while (tries <= retries && !success) {
        const page = await browser.newPage();

        try {
            const response = await page.goto(url);
            if (!response || response.status() !== 200) {
                throw new Error("Failed to fetch page, status:", response.status());
            }

            const jobCriteria = await page.$$("li[class='description__job-criteria-item']");
            if (jobCriteria.length < 4) {
                throw new Error("Job Criteria Not Found!");
            }

            const seniority = (await page.evaluate(element => element.textContent, jobCriteria[0])).replace("Seniority level", "");
            const positionType = (await page.evaluate(element => element.textContent, jobCriteria[1])).replace("Employment type", "");
            const jobFunction = (await page.evaluate(element => element.textContent, jobCriteria[2])).replace("Job function", "");
            const industry = (await page.evaluate(element => element.textContent, jobCriteria[3])).replace("Industries", "");

            const jobData = {
                name: row.name,
                seniority: seniority.trim(),
                position_type: positionType.trim(),
                job_function: jobFunction.trim(),
                industry: industry.trim()
            }
            console.log(jobData)

            success = true;
            console.log("Successfully parsed", row.url);


        } catch (err) {
            tries++;
            console.log(`Error: ${err}, tries left: ${retries-tries}, url: ${getScrapeOpsUrl(url)}`);

        } finally {
            await page.close();
        }
    } 
}

async function processResults(csvFile, location, retries) {
    const rows = await readCsv(csvFile);
    const browser = await puppeteer.launch();;

    for (const row of rows) {
        await processJob(browser, row, location, retries)
    }
    
    await browser.close();

}

async function main() {
    const keywords = ["software engineer"];
    const concurrencyLimit = 5;
    const pages = 1;
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


    console.log("Starting scrape");
    for (const file of aggregateFiles) {
        console.time("processResults");
        await processResults(file, location, retries);
        console.timeEnd("processResults");
    }
    console.log("Scrape complete");
}


main();
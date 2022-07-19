const fs = require('fs-extra')
const path = require('path')

const packageJsonPath = path.join(__dirname, '..', 'planetscale-vtadmin-template', 'package.json')
const buildPath = path.join(__dirname, '..', 'build')
const planetscalePackagePath = path.join(__dirname, '..', 'planetscale-vtadmin')
const indexJsPath = path.join(planetscalePackagePath, 'index.js')
const indexDtsPath = path.join(planetscalePackagePath, 'index.d.ts')

async function main() {
    console.log(`Verifying ${indexJsPath} exists`)
    try {
        await fs.ensureFile(indexJsPath)
    }
    catch (ex) {
        throw new Error(`${indexJsPath} did not exist.`)
    }

    console.log(`Verifying ${indexDtsPath} exists`)
    try {
        await fs.ensureFile(indexDtsPath)
    }
    catch (ex) {
        throw new Error(`${indexDtsPath} did not exist.`)
    }

    console.log(`Copy: ${buildPath} to ${planetscalePackagePath}`)
    try {
        await fs.ensureDir(buildPath)
        await fs.copy(buildPath, planetscalePackagePath)
    }
    catch (e) {
        throw e
    }

    console.log(`Reading package.json from: ${packageJsonPath}`)
    try {
        const packageJsonObj = await fs.readJson(packageJsonPath)
        const { name, version, description, keywords, author, repository, license, publishConfig } = packageJsonObj
        console.log(`Found name: ${name} version: ${version}`)

        const newPackageJson = {
            name,
            version,
            description,
            keywords,
            author,
            repository,
            license,
            homepage: "./",
            publishConfig
        }
        console.log(newPackageJson)

        const newPackageJsonFilePath = path.join(planetscalePackagePath, 'package.json')
        console.log(`Writing new package.json to ${newPackageJsonFilePath}`)
        await fs.writeJson(newPackageJsonFilePath, newPackageJson, { spaces: '  ' })
    }
    catch (e) {
        throw e
    }
}

main()

process.on('unhandledRejection', e => { throw e })

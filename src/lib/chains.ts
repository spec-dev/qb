const invert = (obj1: object): object => {
    const obj2 = {}
    for (const key in obj1) {
        obj2[obj1[key]] = key
    }
    return obj2
}

export const chainIds: { [key: string]: string } = {
    ETHEREUM: '1',
    GOERLI: '5',
    POLYGON: '137',
    MUMBAI: '80001',
    BASE: '8453',
    OPTIMISM: '10',
    ARBITRUM: '42161',
    PGN: '424',
    CELO: '42220',
    LINEA: '59144',
    SEPOLIA: '11155111',
}

export const chainSpecificSchemas = {
    ETHEREUM: 'ethereum',
    GOERLI: 'goerli',
    POLYGON: 'polygon',
    MUMBAI: 'mumbai',
    BASE: 'base',
    OPTIMISM: 'optimism',
    ARBITRUM: 'arbitrum',
    PGN: 'pgn',
    CELO: 'celo',
    LINEA: 'linea',
    SEPOLIA: 'sepolia',
}

export const chainIdForSchema = {
    [chainSpecificSchemas.ETHEREUM]: chainIds.ETHEREUM,
    [chainSpecificSchemas.GOERLI]: chainIds.GOERLI,
    [chainSpecificSchemas.POLYGON]: chainIds.POLYGON,
    [chainSpecificSchemas.MUMBAI]: chainIds.MUMBAI,
    [chainSpecificSchemas.BASE]: chainIds.BASE,
    [chainSpecificSchemas.OPTIMISM]: chainIds.OPTIMISM,
    [chainSpecificSchemas.ARBITRUM]: chainIds.ARBITRUM,
    [chainSpecificSchemas.PGN]: chainIds.PGN,
    [chainSpecificSchemas.CELO]: chainIds.CELO,
    [chainSpecificSchemas.LINEA]: chainIds.LINEA,
    [chainSpecificSchemas.SEPOLIA]: chainIds.SEPOLIA,
}

export const schemaForChainId = invert(chainIdForSchema)

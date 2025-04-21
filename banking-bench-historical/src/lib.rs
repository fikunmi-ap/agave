pub mod snapshot;
pub mod transactions;
pub const SOLANA_NETWORK_CREATION_TIME: i64 = 1584368940;


use std::{fmt::Display, str::FromStr};
use solana_clock::UnixTimestamp;

#[derive(Debug, Clone)]
pub enum Network {
    EclipseTestnet,
    EclipseMainnet,
    SolanaTestnet,
    SolanaMainnet,
}

impl Network {
    pub fn creation_time(&self) -> UnixTimestamp {
        match self {
            Self::EclipseTestnet => 1712572914,
            Self::EclipseMainnet => todo!(),
            Self::SolanaTestnet => 1580834132,
            Self::SolanaMainnet => 1584368940,
        }
    }

    pub fn rpc_url(&self) -> &str {
        match self {
            Self::EclipseTestnet => "https://testnet.dev2.eclipsenetwork.xyz/",
            Self::EclipseMainnet => "https://mainnetbeta-rpc.eclipse.xyz/",
            Self::SolanaTestnet => "https://api.testnet.solana.com",
            Self::SolanaMainnet => "https://api.mainnet-beta.solana.com",   
        }
    }

    pub fn snapshot_url(&self) -> &str {
        match self {
            Self::EclipseTestnet => "https://testnet.dev2.eclipsenetwork.xyz/snapshot.tar.bz2",
            Self::EclipseMainnet => "https://mainnetbeta-rpc.eclipse.xyz/snapshot.tar.bz2",
            Self::SolanaTestnet => "https://api.testnet.solana.com/snapshot.tar.bz2",
            Self::SolanaMainnet => "https://api.mainnet-beta.solana.com/snapshot.tar.bz2",
        }
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::SolanaMainnet
    }
}

impl FromStr for Network {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "eclipse-testnet" => Ok(Self::EclipseTestnet),
            "eclipse-mainnet" => Ok(Self::EclipseMainnet),
            "solana-testnet" => Ok(Self::SolanaTestnet),
            "solana-mainnet" => Ok(Self::SolanaMainnet),
            _ => Err(format!("Unknown Network: {s}")),
        }
    }
}

impl Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EclipseTestnet => write!(f, "eclipse-testnet"),
            Self::EclipseMainnet => write!(f, "eclipse-mainnet"),
            Self::SolanaTestnet => write!(f, "solana-testnet"),
            Self::SolanaMainnet => write!(f, "solana-mainnet"),
        }
    }
}
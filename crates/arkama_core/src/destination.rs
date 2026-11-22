//! Host-selected network boundaries, enforced at URL checks and actual DNS resolution.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

/// Standalone local callers are trusted by default. Servers select `public_only` explicitly.
/// Private exceptions are exact HTTP origins selected by the owner, never by download requests.
#[derive(Clone, Debug, Default)]
pub struct DestinationPolicy {
    restricted: bool,
    private_origins: Vec<Url>,
}

impl DestinationPolicy {
    /// Refuses private/special networks, with at most 32 exact owner-authorized origins.
    pub fn public_only(private_origins: &[String]) -> crate::Result<Self> {
        if private_origins.len() > 32 {
            return Err(crate::Error::download_failed("too many private origins"));
        }
        let mut origins = Vec::new();
        for origin in private_origins {
            let url = Url::parse(origin)
                .map_err(|_| crate::Error::download_failed("invalid private origin"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host().is_none()
                || url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(crate::Error::download_failed(
                    "expected an exact HTTP origin",
                ));
            }
            origins.push(url);
        }
        Ok(Self {
            restricted: true,
            private_origins: origins,
        })
    }

    /// Checks scheme, URL credentials, literal addresses, and exact exception ports/origins.
    /// DNS names are also checked against the actual returned addresses when connecting.
    pub fn validate(&self, url: &Url) -> crate::Result<()> {
        if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
            return Err(crate::Error::download_failed("unsupported destination"));
        }
        if !self.restricted {
            return Ok(());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(crate::Error::download_failed(
                "URL credentials are not allowed",
            ));
        }
        let exception = self
            .private_origins
            .iter()
            .any(|origin| origin.origin() == url.origin());
        // DNS exceptions are hostname-wide at resolution; constrain their ports here, on each hop.
        if !exception
            && self
                .private_origins
                .iter()
                .any(|origin| origin.host() == url.host())
        {
            return Err(crate::Error::download_failed(
                "destination is outside the authorized origin",
            ));
        }
        let ip = match url.host() {
            Some(Host::Ipv4(ip)) => Some(IpAddr::V4(ip)),
            Some(Host::Ipv6(ip)) => Some(IpAddr::V6(ip)),
            _ => None,
        };
        if !exception && ip.is_some_and(|ip| !public_address(ip)) {
            return Err(crate::Error::download_failed(
                "destination address is not allowed",
            ));
        }
        Ok(())
    }

    pub(crate) fn configure(&self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        if self.restricted {
            builder.no_proxy().dns_resolver(Arc::new(self.clone()))
        } else {
            builder
        }
    }
}

impl Resolve for DestinationPolicy {
    fn resolve(&self, name: Name) -> Resolving {
        let hostname = name.as_str().to_owned();
        let exception = self
            .private_origins
            .iter()
            .any(|origin| origin.host_str() == Some(&hostname));
        Box::pin(async move {
            // Check all returned addresses and return that same set to reqwest: no second lookup.
            let addresses: Vec<SocketAddr> = tokio::net::lookup_host((hostname.as_str(), 0))
                .await?
                .collect();
            if addresses.is_empty()
                || (!exception && addresses.iter().any(|addr| !public_address(addr.ip())))
            {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "destination DNS address is not allowed",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192
                    && (b == 168 || (b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            // Only global unicast; exclude transition, protocol-assignment and documentation space.
            (s[0] & 0xe000) == 0x2000
                && s[0] != 0x2002
                && s[0] != 0x3fff
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
        }
    }
}

def domain_of(addr_or_domain: str) -> str:
    s = (addr_or_domain or "").lower().strip()
    return s.split("@")[-1]

def is_internal_domain(addr_or_domain: str, internal_domains) -> bool:
    dom = domain_of(addr_or_domain)
    return any(dom == d or dom.endswith("." + d) for d in internal_domains)
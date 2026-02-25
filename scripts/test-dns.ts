import dns from 'dns/promises';

async function testDomain(domain: string) {
  try {
    const url = new URL(domain);
    const hostname = url.hostname;
    const result = await dns.lookup(hostname);
    console.log('Success:', result);
  } catch (e: any) {
    console.log('Error Name:', e.name);
    console.log('Error Message:', e.message);
    if (e.code) {
      console.log('Error Code:', e.code);
    }
  }
}
testDomain('http://www.corhavenllc.com').catch(console.error);
testDomain('http://www.google.com').catch(console.error);

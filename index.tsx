
import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import App from './App';

// The server answers /api calls with 401 once the sign-in cookie is gone
// (expired, or APP_PASSWORD changed). Send the browser to the sign-in page
// instead of leaving every panel showing its own error.
const originalFetch = window.fetch;
window.fetch = async (input, init) => {
  const res = await originalFetch(input, init);
  const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  const url = new URL(raw, window.location.origin);
  if (res.status === 401 && url.origin === window.location.origin && url.pathname.startsWith('/api/')) {
    window.location.assign(`/api/login?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
  }
  return res;
};

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error("Could not find root element to mount to");
}

const root = ReactDOM.createRoot(rootElement);
root.render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>
);

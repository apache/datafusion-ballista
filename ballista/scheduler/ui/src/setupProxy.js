// Proxy configuration for the development server.
// Previously, this was configured by adding a "proxy" field to package.json, but that stopped working.
// For additional details see https://create-react-app.dev/docs/proxying-api-requests-in-development/#configuring-the-proxy-manually
const { createProxyMiddleware } = require("http-proxy-middleware");

module.exports = function (app) {
  app.use(
    "/api", // the API endpoint
    createProxyMiddleware({
      target: "http://localhost:50050", // the local development backend server
      changeOrigin: true,
    })
  );
};

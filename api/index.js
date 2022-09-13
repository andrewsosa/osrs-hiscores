const express = require("express");
const hiscores = require("osrs-json-hiscores");
const logger = require("morgan");

const app = express();
app.use(logger("dev"));

const handlerServerError = (res) => (err) =>
  res.status(500).send({ status: 500, error: err });

const handleApiResponse = (res) => (hiscores) => {
  if (!!hiscores.length) {
    res.status(200).send(hiscores);
  } else {
    res.status(503).send({ status: 503, error: "Service Unavailable" });
  }
};

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

app.get("/player/:rsn", (req, res) => {
  hiscores
    .getStatsByGamemode(req.params.rsn)
    .then(handleApiResponse(res))
    .catch(handlerServerError(res));
});

app.get("/skill/:skill/", (req, res) => {
  const skill = req.params.skill;
  const mode = req.query.mode || "main";
  const page = parseInt(req.query.page) || 1;

  hiscores
    .getSkillPage(skill, mode, page)
    .then(handleApiResponse(res))
    .catch(handlerServerError(res));
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Example app listening on port ${port}!`));

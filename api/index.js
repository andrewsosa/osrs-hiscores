const express = require("express");
const hiscores = require("osrs-json-hiscores");
const logger = require("morgan");

const app = express();
app.use(logger("dev"));

const handler = (res) => (err) =>
  res.status(404).send({ status: 404, error: err });

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
    .then((response) => res.send(response))
    .catch(handler(res));
});

app.get("/skill/:skill/", (req, res) => {
  const skill = req.params.skill;
  const mode = req.query.mode || "main";
  const page = parseInt(req.query.page) || 1;

  hiscores
    .getSkillPage(skill, mode, page)
    .then((response) => res.send(response))
    .catch(handler(res));
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Example app listening on port ${port}!`));

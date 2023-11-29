const {
  EventHubConsumerClient,
  earliestEventPosition,
} = require("@azure/event-hubs");
const postgres = require("postgres");

const sql = postgres({
  host: "hmdatabase.postgres.database.azure.com:5432",
  user: "hm_admin",
  password: "Guilherme1234@",
  database: "hmdb",
  ssl: {
    rejectUnauthorized: false,
  },
});
require("dotenv").config();

const connectionString =
  "";
const eventHubName = "payment-service";
const consumerGroup = "consumer-group";

async function main() {
  console.log(`Running receiveEvents sample`);

  const consumerClient = new EventHubConsumerClient(
    consumerGroup,
    connectionString,
    eventHubName
  );

  consumerClient.subscribe(
    {
      processEvents: async (events, context) => {
        for (const event of events) {
			await new Promise(resolve => setTimeout(resolve, 10000));
          const paymentData = event.body;
          console.log("paymentData:", paymentData);

          const contaCorrenteIsValid = paymentData.contaCorrente.length === 6;
          const agenciaIsValid = paymentData.agencia.length === 4;
          if (contaCorrenteIsValid && agenciaIsValid) {
            console.log("Pagamento aprovado!");
            await sql`UPDATE pagamento SET status = 'APROVADO' WHERE id = ${paymentData.idPagamento} AND status = 'EM ANDAMENTO'`;
          }
        }
      },
      processError: async (err, context) => {
        console.log(`Error on partition "${context.partitionId}": ${err}`);
      },
    },
    { startPosition: earliestEventPosition }
  );
}

main().catch((error) => {
  console.error("Error running sample:", error);
});

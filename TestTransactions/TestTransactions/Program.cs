using Npgsql;

namespace TestTransactions
{
    internal class Program
    {
        static string GetConnectionString()
        {
            return "Server=localhost;Port=5432;User Id=postgres;Password=1234;Database=postgres;";
        }
        static void Main(string[] args)
        {
            using TextWriter textWriter = new StreamWriter(@"out.txt");
            Console.SetOut(textWriter);

            NpgsqlDataSource dataSource;

            try
            {
                dataSource = NpgsqlDataSource.Create(GetConnectionString());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return;
            }
            InitializeAccountsTable(dataSource);
            Console.WriteLine();

            ReadCommittedTest(dataSource);
            Console.WriteLine();

            RepeatableReadTest(dataSource);
            Console.WriteLine();

            RepeatableReadTest(dataSource, System.Data.IsolationLevel.RepeatableRead);
            Console.WriteLine();

            SerializableTest(dataSource);
            Console.WriteLine();

            SerializableTest(dataSource, System.Data.IsolationLevel.Serializable);
            Console.WriteLine();
        }
        static void InitializeAccountsTable(NpgsqlDataSource dataSource)
        {
            Console.WriteLine("InitializeAccountsTable...");

            using var connection = dataSource.OpenConnection();
            using var command = connection.CreateCommand();

            command.CommandText = "DROP TABLE IF EXISTS accounts";
            command.ExecuteNonQuery();

            command.CommandText = "CREATE TABLE accounts (" +
                "id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY," +
                "name VARCHAR(255)," +
                "balance INT" +
                ")";
            command.ExecuteNonQuery();

            Console.WriteLine("InitializeAccountsTable ends");
        }
        static void ReadCommittedTest(NpgsqlDataSource dataSource)
        {
            Console.WriteLine("ReadCommitedTest...");

            Task transaction1Task = Task.Run(() =>
            {
                using var connection = dataSource.OpenConnection();
                using var transaction = connection.BeginTransaction();
                Console.WriteLine("Transaction 1: start");
                using var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO accounts(name, balance) VALUES('Olga', 100000);";
                command.ExecuteNonQuery();
                Console.WriteLine("Transaction 1: Olga insert");

                GoToSleep("Transaction 1", 1500);

                command.CommandText = "INSERT INTO accounts(name, balance) VALUES('Bob', 1);";
                command.ExecuteNonQuery();
                Console.WriteLine("Transaction 1: Bob insert");

                transaction.Commit();
                Console.WriteLine("Transaction 1: commit");
            });

            Task transaction2Task = Task.Run(() =>
            {
                GoToSleep("Transaction 2", 100);

                using var connection = dataSource.OpenConnection();
                using var transaction = connection.BeginTransaction();
                Console.WriteLine("Transaction 2: start");
                using var command = connection.CreateCommand();

                command.CommandText = "SELECT id, name, balance FROM accounts;";

                Console.WriteLine("Transaction 2: reading rows...");
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt32(0);
                        var name = reader.GetString(1);
                        var balance = reader.GetInt32(2);

                        Console.WriteLine($"Transaction 2: {id} {name} {balance}");
                    }
                };

                transaction.Commit();
                Console.WriteLine("Transaction 2: commit");
            });

            Task.WaitAll(transaction1Task, transaction2Task);
            Console.WriteLine("ReadCommitedTest ends");
        }
        static void RepeatableReadTest(NpgsqlDataSource dataSource, System.Data.IsolationLevel isolationLevel = System.Data.IsolationLevel.ReadCommitted)
        {
            Console.WriteLine($"{nameof(RepeatableReadTest)}, isolation level = {isolationLevel}...");
            Task insertNewRowsTask = new Task(() =>
            {
                using var connection = dataSource.OpenConnection();
                using var transaction = connection.BeginTransaction();
                Console.WriteLine("Inserting transaction: start");
                using var command = connection.CreateCommand();

                command.CommandText = "INSERT INTO accounts(name, balance) VALUES('Erik', 78);";
                command.ExecuteNonQuery();
                Console.WriteLine("Inserting transaction: Erik insert");

                command.CommandText = "INSERT INTO accounts(name, balance) VALUES('Jenna', 398);";
                command.ExecuteNonQuery();
                Console.WriteLine("Inserting transaction: Jenna insert");

                transaction.Commit();
                Console.WriteLine("Inserting transaction: commit");
            });

            Task transactionTask = Task.Run(() =>
            {
                using var connection = dataSource.OpenConnection();
                using var transaction = connection.BeginTransaction(isolationLevel);
                Console.WriteLine("Transaction: start");
                using var command = connection.CreateCommand();

                command.CommandText = "SELECT id, name, balance FROM accounts;";

                Console.WriteLine("Transaction: reading rows...");
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt32(0);
                        var name = reader.GetString(1);
                        var balance = reader.GetInt32(2);

                        Console.WriteLine($"Transaction: {id} {name} {balance}");
                    }
                };

                insertNewRowsTask.Start();
                insertNewRowsTask.Wait();

                Console.WriteLine("Transaction: reading rows again...");
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt32(0);
                        var name = reader.GetString(1);
                        var balance = reader.GetInt32(2);

                        Console.WriteLine($"Transaction: {id} {name} {balance}");
                    }
                };

                transaction.Commit();
                Console.WriteLine("Transaction: commit");
            });

            transactionTask.Wait();
            Console.WriteLine($"{nameof(RepeatableReadTest)} ends");
        }
        static void SerializableTest(NpgsqlDataSource dataSource, System.Data.IsolationLevel isolationLevel = System.Data.IsolationLevel.ReadCommitted)
        {
            Console.WriteLine($"{nameof(SerializableTest)}, isolation level = {isolationLevel}...");

            Task transaction1Task = Task.Run(() =>
            {
                using var connection = dataSource.OpenConnection();
                using var transaction = connection.BeginTransaction(isolationLevel);
                Console.WriteLine("Transaction 1: start");
                using var command = connection.CreateCommand();

                try
                {
                    command.CommandText = "UPDATE accounts SET balance = balance - 100 WHERE ID = 1;";
                    command.ExecuteNonQuery();
                    Console.WriteLine("Transaction 1: Olga`s balance updated: -100");

                    GoToSleep("Transaction 1", 100);

                    Console.WriteLine("Transaction 1: reading row WHERE ID = 1...");
                    command.CommandText = "SELECT id, name, balance FROM accounts WHERE ID = 1;";
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = reader.GetInt32(0);
                            var name = reader.GetString(1);
                            var balance = reader.GetInt32(2);

                            Console.WriteLine($"Transaction 1: {id} {name} {balance}");
                        }
                    };

                    transaction.Commit();
                    Console.WriteLine("Transaction 1: commit");
                }
                catch(NpgsqlException e)
                {
                    Console.WriteLine(e.Message);
                }
            });

            Task transaction2Task = Task.Run(() =>
            {
                using var connection = dataSource.OpenConnection();
                using var transaction = connection.BeginTransaction(isolationLevel);
                Console.WriteLine("Transaction 2: start");
                using var command = connection.CreateCommand();

                try
                {
                    command.CommandText = "UPDATE accounts SET balance = balance - 50 WHERE ID = 1;";
                    command.ExecuteNonQuery();
                    Console.WriteLine("Transaction 2: Olga`s balance updated: -50");

                    GoToSleep("Transaction 2", 100);

                    Console.WriteLine("Transaction 2: reading row WHERE ID = 1...");
                    command.CommandText = "SELECT id, name, balance FROM accounts WHERE ID = 1;";
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = reader.GetInt32(0);
                            var name = reader.GetString(1);
                            var balance = reader.GetInt32(2);

                            Console.WriteLine($"Transaction 2: {id} {name} {balance}");
                        }
                    };

                    transaction.Commit();
                    Console.WriteLine("Transaction 2: commit");
                }
                catch (NpgsqlException e)
                {
                    Console.WriteLine(e.Message);
                }
            });

            Task.WaitAll(transaction1Task, transaction2Task);
            Console.WriteLine($"{nameof(SerializableTest)} ends");
        }
        static void GoToSleep(String caller, int millis)
        {
            Console.WriteLine($"{caller} fell asleep for {millis} milliseconds...");
            Thread.Sleep(millis);
        }
    }
}

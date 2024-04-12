using System.Net;

namespace TestovoeEM
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                var files = Directory.GetFiles(Directory.GetCurrentDirectory()).Where(path => path.EndsWith("config"));
                if (files.Any())
                {
                    args = File.ReadAllText(files.First()).Split([Environment.NewLine, " "], StringSplitOptions.RemoveEmptyEntries);
                }
                else
                {
                    Console.WriteLine("Usage: IPAddressAnalyzer --file-log <log_file_path> --file-output <output_file_path> [--address-start <start_address> --address-mask <address_mask> --time-start <start_time> --time-end <end_time>]");
                    return;
                }
            }
            try
            {
                // Парсинг аргументов
                var (fileLog, fileOutput, addressStart, addressMask, timeStart, timeEnd) = ParseArguments(args);

                // Загрузка файла логов
                var logEntries = LoadLogEntries(fileLog);

                // Фильтрация записей
                List<(IPAddress Address, DateTime Time)> filteredLogEntries = new List<(IPAddress Address, DateTime Time)> ();
                var loopBack = Parallel.ForEach(logEntries, (entry) =>
                {
                    if (entry.Time < timeStart || entry.Time > timeEnd)
                    {
                        return;
                    }
                    if (addressStart != null)
                    {
                        if (entry.Address.IsInRange(addressStart, addressMask != null ? CalculateEndAddress(addressStart, addressMask) : IPAddress.Broadcast))
                        {
                            filteredLogEntries.Add(entry);
                        }
                    }
                    else
                    {
                        filteredLogEntries.Add(entry);
                    }
                });
                while (!loopBack.IsCompleted) { }

                // Подсчет количества обращений по адресам
                var ipAddressCounts = CountIpAddresses(filteredLogEntries);

                // Запись результатов в файл
                WriteResultsToFile(ipAddressCounts, fileOutput);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed with this message: {ex.Message}");
            }
        }

        static (string FileLog, string FileOutput, IPAddress AddressStart, IPAddress AddressMask, DateTime TimeStart, DateTime TimeEnd) ParseArguments(string[] args)
        {
            // Инициализация значений по умолчанию
            string fileLog = null;
            string fileOutput = null;
            IPAddress addressStart = null;
            IPAddress addressMask = null;
            DateTime timeStart = DateTime.MinValue;
            DateTime timeEnd = DateTime.MaxValue;

            // Парсинг аргументов
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--file-log" or "file-log":
                        fileLog = i + 1 < args.Length ? args[++i] : throw new ArgumentException("Missing value for --file-log argument");
                        break;
                    case "--file-output" or "file-output":
                        fileOutput = i + 1 < args.Length ? args[++i] : throw new ArgumentException("Missing value for --file-output argument");
                        break;
                    case "--address-start" or "address-start":
                        if (i + 1 < args.Length)
                        {
                            try
                            {
                                addressStart = IPAddress.Parse(args[++i]);
                            }
                            catch (FormatException)
                            {
                                throw new ArgumentException($"Invalid IP address format for --address-start: {args[i]}");
                            }
                        }
                        else
                        {
                            throw new ArgumentException("Missing value for --address-start argument");
                        }
                        break;
                    case "--address-mask" or "address-mask":
                        if (i + 1 < args.Length)
                        {
                            try
                            {
                                addressMask = IPAddress.Parse(args[++i]);
                            }
                            catch (FormatException)
                            {
                                throw new ArgumentException($"Invalid IP address format for --address-mask: {args[i]}");
                            }
                        }
                        else
                        {
                            throw new ArgumentException("Missing value for --address-mask argument");
                        }
                        break;
                    case "--time-start" or "time-start":
                        if (i + 1 < args.Length)
                        {
                            try
                            {
                                timeStart = DateTime.ParseExact(args[++i], "dd.MM.yyyy", null);
                            }
                            catch (FormatException)
                            {
                                throw new ArgumentException($"Invalid time format for --time-start: {args[i]}");
                            }
                        }
                        else
                        {
                            throw new ArgumentException("Missing value for --time-start argument");
                        }
                        break;
                    case "--time-end" or "time-end":
                        if (i + 1 < args.Length)
                        {
                            try
                            {
                                timeEnd = DateTime.ParseExact(args[++i], "dd.MM.yyyy", null);
                            }
                            catch (FormatException)
                            {
                                throw new ArgumentException($"Invalid time format for --time-end: {args[i]}");
                            }
                        }
                        else
                        {
                            throw new ArgumentException("Missing value for --time-end argument");
                        }
                        break;
                }
            }
            if (fileLog == null)
            {
                throw new ArgumentException("Missing required argument: --file-log");
            }
            if (fileOutput == null)
            {
                throw new ArgumentException("Missing required argument: --file-output");
            }

            return (fileLog, fileOutput, addressStart, addressMask, timeStart, timeEnd);
        }

        static List<(IPAddress Address, DateTime Time)> LoadLogEntries(string filePath)
        {
            var logEntries = new List<(IPAddress Address, DateTime Time)>();

            try
            {
                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException("Log file not found", filePath);
                }

                using (var reader = new StreamReader(filePath))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        var parts = line.Split(":", 2);
                        if (parts.Length != 2)
                        {
                            throw new FormatException($"Invalid log entry format: {line}"); 
                        }
                        try
                        {
                            var address = IPAddress.Parse(parts[0]);
                            var time = DateTime.ParseExact(parts[1], "yyyy-MM-dd HH:mm:ss", null);
                            logEntries.Add((address, time));
                        }
                        catch (FormatException ex)
                        {
                            throw new FormatException($"Error parsing log entry: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error reading log file: {ex.Message}");
            }

            return logEntries;
        }

        static IPAddress CalculateEndAddress(IPAddress startAddress, IPAddress mask)
        {
            var startBytes = startAddress.GetAddressBytes();
            var maskBytes = mask.GetAddressBytes();

            var endBytes = new byte[startBytes.Length];
            for (int i = 0; i < startBytes.Length; i++)
            {
                endBytes[i] = (byte)(startBytes[i] | ~maskBytes[i]);
            }

            return new IPAddress(endBytes);
        }

        static Dictionary<IPAddress, int> CountIpAddresses(List<(IPAddress Address, DateTime Time)> logEntries)
        {
            var ipAddressCounts = new Dictionary<IPAddress, int>();

            foreach (var entry in logEntries)
            {
                if (ipAddressCounts.ContainsKey(entry.Address))
                    ipAddressCounts[entry.Address]++;
                else
                    ipAddressCounts[entry.Address] = 1;
            }

            return ipAddressCounts;
        }

        static void WriteResultsToFile(Dictionary<IPAddress, int> ipAddressCounts, string filePath)
        {
            try
            {
                using (var writer = new StreamWriter(filePath))
                {
                    foreach (var entry in ipAddressCounts)
                    {
                        writer.WriteLine($"{entry.Key}: {entry.Value}");
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error writing output file: {ex.Message}");
            }
        }
    }

    public static class IPAddressExtensions
    {
        public static bool IsInRange(this IPAddress address, IPAddress startAddress, IPAddress endAddress)
        {
            var startBytes = startAddress.GetAddressBytes();
            var endBytes = endAddress.GetAddressBytes();
            var targetBytes = address.GetAddressBytes();

            bool lowerBoundary = true, upperBoundary = true;

            for (int i = 0; i < startBytes.Length && (lowerBoundary || upperBoundary); i++)
            {
                if ((lowerBoundary && targetBytes[i] < startBytes[i]) || (upperBoundary && targetBytes[i] > endBytes[i]))
                    return false;

                if (targetBytes[i] != startBytes[i])
                    lowerBoundary = false;

                if (targetBytes[i] != endBytes[i])
                    upperBoundary = false;
            }

            return true;
        }
    }
}

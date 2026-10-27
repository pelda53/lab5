import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogPipelineProcessor {
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+) \\S+ \\S+ \\[(.*?)\\] \"([A-Z]+) (.*?)(?: HTTP/[0-9.]+)?\" (\\d{3}) (-|\\d+) \"([^\"]*)\" \"([^\"]*)\"$"
    );

    private static final DateTimeFormatter DATE_FORMATTER = 
        DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    public static void processLogFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println("Dosya bulunamadı: " + filePath);
            return;
        }

        long totalRead = 0;
        long emittedCount = 0;
        long droppedCount = 0;
        long deadLetterCount = 0;

        System.out.println("=== İşleniyor: " + file.getName() + " ===");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.ISO_8859_1))) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                totalRead++;
                byte[] rawBytes = line.getBytes(StandardCharsets.UTF_8);

                // 1. Kural: 100 KiB (102400 bayt) aşırı uzun satır kontrolü
                if (rawBytes.length > 102400) {
                    deadLetterCount++;
                    continue;
                }

                // 2. Kural: Non-UTF-8 karakter / kodlama hatası kontrolü
                try {
                    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                    decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPORT);
                    decoder.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
                    decoder.decode(ByteBuffer.wrap(rawBytes));
                } catch (Exception e) {
                    deadLetterCount++;
                    continue;
                }

                // 3. Kural: Regex format eşleşmesi (Syntax)
                Matcher matcher = LOG_PATTERN.matcher(line);
                if (!matcher.matches()) {
                    droppedCount++;
                    continue;
                }

                String sizeStr = matcher.group(6);

                // 4. Kural: Negatif yanıt boyutu kontrolü
                if (!sizeStr.equals("-")) {
                    try {
                        long size = Long.parseLong(sizeStr);
                        if (size < 0) {
                            droppedCount++;
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        droppedCount++;
                        continue;
                    }
                }

                // 5. Kural: Zaman damgası ve yıl aralığı kontrolü (1970 ve 2099 filtresi)
                String dateStr = matcher.group(2);
                try {
                    OffsetDateTime dateTime = OffsetDateTime.parse(
                        dateStr.replace(':', ' '), 
                        DATE_FORMATTER
                    );
                    int year = dateTime.getYear();
                    if (year < 2020 || year > 2030) {
                        deadLetterCount++;
                        continue;
                    }
                } catch (Exception e) {
                    droppedCount++;
                    continue;
                }

                // Başarılı kayıtlar (Main Sink)
                emittedCount++;
            }
        } catch (Exception e) {
            System.out.println("Dosya okunurken hata oluştu: " + e.getMessage());
        }

        System.out.println("Toplam Okunan Kayıt  : " + totalRead);
        System.out.println("Emitted (Başarılı)   : " + emittedCount);
        System.out.println("Dropped (Atlanan)    : " + droppedCount);
        System.out.println("Dead-Letter (Ölü)    : " + deadLetterCount);
        System.out.println("Mutabakat (Toplam)   : " + (emittedCount + droppedCount + deadLetterCount));
        System.out.println("----------------------------------------\n");
    }

    public static void main(String[] args) {
        processLogFile("access-dirty.log");
        processLogFile("access-empty.log");
    }
}

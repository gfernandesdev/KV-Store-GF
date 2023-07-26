import java.io.Serializable;
//
public class Mensagem implements Serializable {
    String comando;
    String key;
    String value;
    String response;
    long timestamp;

    public Mensagem(String comando, String key, String value) {
        this.comando = comando;
        this.key = key;
        this.value = value;
    }

    public Mensagem(String comando, String key) {
        this.comando = comando;
        this.key = key;
    }

    public Mensagem(String response) {
        this.response = response;
    }

    @Override
    public String toString() {
        // Retorna uma representação personalizada da mensagem
        // Neste exemplo, estou retornando a string formatada com comando, key e value
        return "Mensagem{" +
                "comando='" + comando + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}

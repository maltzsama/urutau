# EncodeKey — design e invariante (substitui a tabela `n:`/`f:` da v4)

> **Status:** implementado em `internal/dataplane/generator.go` (`EncodeKey`).
> **Supersede:** a seção H-1 do `DP-AUDIT v4` propunha prefixos por
> **espaço de valor** (`n:`, `f:`, `s:`, `bin:`) com estabilidade sob
> widening (`int32(5)` e `int64(5)` → mesma chave). A implementação usa
> **type-tags binários com payload de largura fixa**. Este doc registra o
> desvio aceito, o invariante que o código garante e o caveat que o torna
> seguro.

## 1. O que mudou da v4

| | v4 (espaço de valor) | implementado (type-tag) |
|---|---|---|
| Encoding | `[prefixo textual][decimal/hex]` | `[1 byte tag][payload binário]` |
| `int32(5)` vs `int64(5)` | mesma chave (`n:5`) | **chaves diferentes** (tags `0x01` vs `0x02`) |
| `float32(0.1)` vs `float64(0.1)` | mesma chave (promoção p/ f64) | **chaves diferentes** (bit patterns distintos) |
| NaN / ±0 | NaN → erro (M-11) | NaN/±0 comparam por bit pattern — determinístico, M-11 morre |
| Performance | `fmt.Sprintf`/`FormatInt` por valor | append binário direto, sem formatação |

## 2. Por que o type-tag é melhor aqui

O consumidor real é o **Collapse: por batch, em memória**. A chave:

- **nunca persiste** — vive só dentro de uma chamada de Collapse;
- **nunca cruza batches** — um batch tem um tipo fixo por coluna;
- **nunca cruza versões de schema** — não há comparação int32↔int64 real.

Nesse regime, o argumento do widening da v4 ("ALTER INT→BIGINT splita uma
linha lógica") não se materializa: a mudança de tipo de uma coluna PK
acontece **entre** batches, e a chave nunca sobrevive ao batch. O type-tag:

1. é **mais injetivo** — todo tipo tem tag própria, sem sobreposição de
   espaços (`bin:` cobria Binary/FSB/UUID com bytes crus; tags distintos
   os separam mesmo com bytes idênticos);
2. é **mais rápido** — sem formatação decimal de inteiros/floats;
3. torna NaN/±0 **determinísticos por bit pattern** de graça.

## 3. O invariante (o que o código garante)

> `key(a) == key(b) ⟺ valor(a) == valor(b)` **dentro do mesmo tipo** — e o
> tipo de cada coluna PK é constante dentro de um batch.

Cada campo é `[type-byte][payload]`; o payload é length-prefixed onde a
largura varia (string/binary/decimal) e de largura fixa no resto. O
length-prefix por valor mata a colisão por concatenação
(`"ab"+"c" == "a"+"bc"` — o adversário dedicado do CR-069 §3.2).

Tabela de tags (ver `generator.go`):

| Tag | Tipo Arrow | Payload |
|---|---|---|
| `0x01`–`0x07` | Int32, Int64, Uint64, Float32, Float64, Bool, String | LE width-fixed; string length-prefixed |
| `0x08` | Decimal128 | `ValueStr` canônico, length-prefixed |
| `0x09`/`0x0A` | Date32, Time64 | LE 4/8 bytes |
| `0x0B` | Timestamp (qualquer TZ/unidade) | LE 8 bytes |
| `0x0C` | Binary | length-prefixed |
| `0x0D` | FixedSizeBinary (UUID) | raw width-fixed |

Null em coluna PK é **erro** — `EncodeKey` é a autoridade única de
validade da chave (M-9); o Collapse não pré-checa.

## 4. O caveat (o que torna o desvio seguro)

> **Chaves são efêmeras.** Se um dia a chave precisar **persistir** ou ser
> **comparada entre batches/versões de schema**, este design volta à mesa:
> sob type-tags, um widening int32→int64 muda a chave da mesma linha
> lógica — exatamente o cenário que motivou o prefixo por espaço de valor
> da v4.

Esse registro é a condição de aceitação do desvio (revisão do reviewer,
T-9 revisado): a sub-case "estabilidade sob widening" da v4 **morre por
design** — `TestEncodeKeyTypeAntiCollision` agora asserta o contrário
(`int32(5) ≠ int64(5)`, `float32(0.1) ≠ float64(0.1)`, UUID ≠ Binary com
bytes idênticos).

## 5. Assinatura e complexidade

```go
func EncodeKey(record arrow.RecordBatch, row int, pkIdxs []int, pkCols []string) ([]byte, error)
```

- `pkIdxs` são resolvidos **uma vez** pelo caller (M-9) — sem lookup de
  nome por linha (era O(rows×cols));
- `pkCols` existem só para mensagens de erro.

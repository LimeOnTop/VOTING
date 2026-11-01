import React, { useState, useEffect } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";
type Poll = {
  id: string;
  question: string;
  type: string;
  options: { id: string; label: string }[];
  opens_at: string;
  closes_at: string;
};
type Result = {
  poll: Poll;
  voters: number;
  counts: Record<string, number>;
  updated_at: string | null;
};
async function api<T>(path: string, token = "", body?: unknown): Promise<T> {
  const r = await fetch(path, {
    credentials: "same-origin",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    ...(body ? { method: "POST", body: JSON.stringify(body) } : {}),
  });
  const d = await r.json();
  if (!r.ok) throw Object.assign(Error(d.error || "Сервис недоступен"), {status: r.status});
  return d;
}
const date = (s: string) =>
  new Date(s).toLocaleString("ru-RU", {
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
function Voting() {
  const id = location.pathname.startsWith("/p/")
    ? location.pathname.split("/")[2]
    : "";
  const [p, setP] = useState<Poll>();
  const [selected, setSelected] = useState<string[]>([]);
  const [phase, setPhase] = useState("loading");
  const [error, setError] = useState("");
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!id) {
      setPhase("home");
      return;
    }
    let live = true;
    Promise.all([api<Poll>(`/api/polls/${id}`), api("/api/session")])
      .then(async ([poll]) => {
        const receipt = await api<{ status: string; options?: string[] }>(
          `/api/polls/${id}/receipt`,
        );
        if (live) {
          setP(poll);
          setSelected(receipt.options || []);
          setPhase(receipt.status === "accepted" ? "accepted" : "ready");
        }
      })
      .catch((e) => {
        if (live) {
          setError(e.message);
          setPhase("error");
        }
      });
    return () => {
      live = false;
    };
  }, [id]);
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  async function submit() {
    setError("");
    setPhase("sending");
    try {
      await api(`/api/polls/${id}/votes`, "", { options: selected });
      setPhase("accepted");
    } catch (e) {
      if ((e as Error & {status?:number}).status === 409) {
        const receipt = await api<{options?:string[]}>(`/api/polls/${id}/receipt`).catch(()=>undefined);
        setSelected(receipt?.options || []);
        setPhase("accepted");
        return;
      }
      setError((e as Error).message);
      setPhase("ready");
    }
  }
  if (phase === "home")
    return (
      <main className="landing">
        <div className="kicker">Общественное мнение начинается с вас</div>
        <h1>
          Один вопрос.
          <br />
          <em>Ваш голос.</em>
        </h1>
        <p className="intro">
          Увидели опрос в эфире? Перейдите по ссылке или отсканируйте QR-код на
          экране телевизора.
        </p>
        <p className="muted">Регистрация не нужна. Просто выберите ответ.</p>
        <a className="textlink" href="/admin">
          Создать опрос →
        </a>
      </main>
    );
  if (!p)
    return (
      <main className="vote-shell">
        <p role="status">{error || "Открываем опрос…"}</p>
      </main>
    );
  const open = now >= Date.parse(p.opens_at) && now < Date.parse(p.closes_at),
    left = Math.max(0, Math.ceil((Date.parse(p.closes_at) - now) / 1000)),
    busy = phase === "sending";
  return (
    <main className="vote-shell">
      <div className="poll-top">
        <span className="kicker">Вопрос эфира</span>
        <span className="status">
          {open
            ? `До конца ${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}`
            : now < Date.parse(p.opens_at)
              ? "Скоро в эфире"
              : "Голосование завершено"}
        </span>
      </div>
      <h1 className="question">{p.question}</h1>
      <p className="muted">
        {p.type === "multiple"
          ? "Можно выбрать несколько вариантов"
          : "Выберите один вариант ответа"}
      </p>
      {phase === "accepted" ? (
        <div className="success" role="status">
          <span className="check">✓</span>
          <h2>Ваш голос принят</h2>
          <p>Спасибо! Ответ сохранён и будет включён в результаты после обработки.</p>
          <p className="muted">
            Ваш ответ:{" "}
            {p.options
              .filter((o) => selected.includes(o.id))
              .map((o) => o.label)
              .join(", ")}
          </p>
        </div>
      ) : (
        <>
          <fieldset disabled={!open || busy} className="options">
            <legend className="sr-only">Варианты ответа</legend>
            {p.options.map((o, i) => (
              <label
                className={
                  "option " + (selected.includes(o.id) ? "selected" : "")
                }
                key={o.id}
              >
                <span className="option-num">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <span>{o.label}</span>
                <input
                  type={p.type === "single" ? "radio" : "checkbox"}
                  name="answer"
                  checked={selected.includes(o.id)}
                  onChange={() =>
                    setSelected((prev) =>
                      p.type === "single"
                        ? [o.id]
                        : prev.includes(o.id)
                          ? prev.filter((x) => x !== o.id)
                          : [...prev, o.id],
                    )
                  }
                />
              </label>
            ))}
          </fieldset>
          {error && (
            <p className="error" role="alert">
              {error}
            </p>
          )}
          <button
            className="primary vote-button"
            disabled={!open || !selected.length || busy}
            onClick={submit}
          >
            {phase === "sending"
              ? "Сохраняем…"
              : busy
                ? "Голос принят в обработку"
                : "Проголосовать"}
            <span>→</span>
          </button>
          <p className="privacy">
            Один голос с браузера. Личные данные не запрашиваются.
          </p>
        </>
      )}
    </main>
  );
}
function Admin() {
  const [token, setToken] = useState(""),
    [draft, setDraft] = useState(""),
    [polls, setPolls] = useState<Poll[]>([]),
    [active, setActive] = useState(""),
    [result, setResult] = useState<Result>(),
    [error, setError] = useState(""),
    [creating, setCreating] = useState(false),
    [refresh, setRefresh] = useState(0);
  useEffect(() => {
    if (!token) return;
    let live = true;
    api<Poll[]>("/api/admin/polls", token)
      .then((p) => {
        if (live) {
          setPolls(p);
          setActive((a) => a || p[0]?.id || "");
        }
      })
      .catch((e) => {
        if (live) setError(e.message);
      });
    return () => {
      live = false;
    };
  }, [token, refresh]);
  useEffect(() => {
    if (!token || !active) return;
    let live = true;
    setResult(undefined);
    const load = () =>
      api<Result>(`/api/admin/polls/${active}/results`, token)
        .then((r) => {
          if (live) {
            setResult(r);
            setError("");
          }
        })
        .catch((e) => {
          if (live) setError(e.message);
        });
    load();
    const t = setInterval(load, 3000);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, [token, active, refresh]);
  async function login(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    try {
      await api("/api/admin/polls", draft);
      setToken(draft);
      setDraft("");
    } catch (e) {
      setError((e as Error).message);
    }
  }
  if (!token)
    return (
      <main className="login">
        <span className="kicker">Для организаторов</span>
        <h1>
          Управление
          <br />
          опросами
        </h1>
        <p className="muted">
          Введите ключ администратора из настроек сервиса.
        </p>
        <form onSubmit={login}>
          <label>
            Ключ доступа
            <input
              type="password"
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              autoComplete="current-password"
              required
              minLength={24}
            />
          </label>
          {error && (
            <p className="error" role="alert">
              {error}
            </p>
          )}
          <button className="primary">
            Войти <span>→</span>
          </button>
        </form>
      </main>
    );
  const p = polls.find((p) => p.id === active);
  return (
    <main className="admin-shell">
      <div className="admin-heading">
        <div>
          <span className="kicker">Рабочее пространство</span>
          <h1>
            Ваши опросы <small>/ {polls.length}</small>
          </h1>
        </div>
        <div className="actions">
          <button
            className="secondary"
            onClick={() => {
              setToken("");
              setPolls([]);
              setResult(undefined);
            }}
          >
            Выйти
          </button>
          <button className="primary" onClick={() => setCreating(true)}>
            + Создать опрос
          </button>
        </div>
      </div>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <div className="admin-grid">
        <aside aria-label="Список опросов">
          {!polls.length && (
            <p className="muted">
              Создайте первый опрос, чтобы получить ссылку для зрителей.
            </p>
          )}
          {polls.map((p) => (
            <button
              key={p.id}
              className={"poll-item " + (p.id === active ? "current" : "")}
              onClick={() => setActive(p.id)}
            >
              <span className="muted">{date(p.opens_at)}</span>
              <strong>{p.question}</strong>
              <span>
                {Date.now() < Date.parse(p.closes_at)
                  ? "Запланирован / в эфире"
                  : "Завершён"}{" "}
                →
              </span>
            </button>
          ))}
        </aside>
        <section className="results">
          {p ? (
            <>
              <div className="result-header">
                <span className="kicker">Результаты опроса</span>
                <span className="muted">Обновление каждые 3 сек</span>
              </div>
              <h2>{p.question}</h2>
              <div className="total">
                <strong>{result ? result.voters.toLocaleString("ru-RU") : "—"}</strong>
                <span>учтённых голосов</span>
              </div>
              <div className="bars">
                {p.options.map((o) => {
                  const count = result?.counts[o.id],
                    pct = result?.voters ? ((count ?? 0) / result.voters) * 100 : 0;
                  return (
                    <div className="bar-item" key={o.id}>
                      <div>
                        <span>{o.label}</span>
                        <strong>
                          {result ? `${pct.toFixed(1)}%` : "—"}{" "}
                          <small>{count?.toLocaleString("ru-RU") ?? "—"}</small>
                        </strong>
                      </div>
                      <div className="bar-track">
                        <div style={{ width: `${pct}%` }} />
                      </div>
                    </div>
                  );
                })}
              </div>
              <p className="muted">
                {p.type === "multiple"
                  ? "Доля от участников; сумма процентов может превышать 100%."
                  : "Доля от числа участников."}
              </p>
              <div className="share">
                <label>
                  Ссылка для зрителей
                  <input
                    readOnly
                    value={`${location.origin}/p/${p.id}`}
                    onFocus={(e) => e.target.select()}
                  />
                </label>
                <a href={`/p/${p.id}`} target="_blank" rel="noreferrer">
                  Открыть ↗
                </a>
              </div>
              <p className="muted">
                Приём: {date(p.opens_at)} — {date(p.closes_at)}.<br />
                {result?.updated_at
                  ? `Последний учтённый голос: ${date(result.updated_at)}.`
                  : result
                    ? "Пока нет учтённых голосов."
                    : error
                      ? "Результаты временно недоступны."
                      : "Результаты загружаются."}{" "}
                Подсчёт асинхронный.
              </p>
            </>
          ) : (
            <div className="empty">
              <h2>Здесь будет мнение зрителей</h2>
              <p className="muted">Создайте опрос и поделитесь ссылкой.</p>
            </div>
          )}
        </section>
      </div>
      {creating && (
        <CreateModal
          token={token}
          close={() => setCreating(false)}
          created={(p) => {
            setCreating(false);
            setActive(p.id);
            setRefresh((x) => x + 1);
          }}
        />
      )}
    </main>
  );
}
function CreateModal({
  token,
  close,
  created,
}: {
  token: string;
  close: () => void;
  created: (p: Poll) => void;
}) {
  const [question, setQuestion] = useState(""),
    [options, setOptions] = useState(["", ""]),
    [type, setType] = useState("single"),
    [minutes, setMinutes] = useState(1),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const now = Date.now();
      created(
        await api<Poll>("/api/admin/polls", token, {
          question,
          type,
          options,
          opens_at: new Date(now).toISOString(),
          closes_at: new Date(now + minutes * 60000).toISOString(),
        }),
      );
    } catch (e) {
      setError((e as Error).message);
      setBusy(false);
    }
  }
  return (
    <div className="overlay">
      <section
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-title"
      >
        <div className="modal-title">
          <h2 id="create-title">Новый опрос</h2>
          <button className="secondary" onClick={close} aria-label="Закрыть">
            ×
          </button>
        </div>
        <form onSubmit={submit}>
          <label>
            Вопрос
            <textarea
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              required
              maxLength={300}
              autoFocus
              placeholder="Что вы думаете о…?"
            />
          </label>
          <div className="form-row">
            <label>
              Тип ответа
              <select value={type} onChange={(e) => setType(e.target.value)}>
                <option value="single">Один вариант</option>
                <option value="multiple">Несколько вариантов</option>
              </select>
            </label>
            <label>
              Длительность, минут
              <input
                type="number"
                min={1}
                max={1440}
                value={minutes}
                onChange={(e) => setMinutes(Number(e.target.value))}
                required
              />
            </label>
          </div>
          <fieldset>
            <legend>Варианты ответа</legend>
            {options.map((o, i) => (
              <label key={i} className="option-edit">
                <span>{i + 1}</span>
                <input
                  aria-label={`Вариант ${i + 1}`}
                  value={o}
                  onChange={(e) =>
                    setOptions((prev) =>
                      prev.map((v, n) => (n === i ? e.target.value : v)),
                    )
                  }
                  maxLength={120}
                  required
                />
                {options.length > 2 && (
                  <button
                    type="button"
                    className="secondary"
                    aria-label={`Удалить вариант ${i + 1}`}
                    onClick={() =>
                      setOptions((prev) => prev.filter((_, n) => n !== i))
                    }
                  >
                    ×
                  </button>
                )}
              </label>
            ))}
            {options.length < 10 && (
              <button
                type="button"
                className="textlink"
                onClick={() => setOptions([...options, ""])}
              >
                + Добавить вариант
              </button>
            )}
          </fieldset>
          {error && (
            <p className="error" role="alert">
              {error}
            </p>
          )}
          <p className="muted">
            Опрос откроется сразу. Изменить варианты после публикации нельзя.
          </p>
          <button className="primary" disabled={busy}>
            {busy ? "Создаём…" : "Опубликовать опрос"}
            <span>→</span>
          </button>
        </form>
      </section>
    </div>
  );
}
function App() {
  const admin = location.pathname.startsWith("/admin");
  return (
    <>
      <header>
        <a className="brand" href="/">
          V<span>O</span>TING.
        </a>
        <span className="header-note">Национальная платформа опросов</span>
        <a className="navlink" href={admin ? "/" : "/admin"}>
          {admin ? "На главную" : "Для организаторов ↗"}
        </a>
      </header>
      {admin ? <Admin /> : <Voting />}
      <footer>
        <span>Каждый голос имеет значение.</span>
        <span>Без регистрации · Без персональных данных в результатах</span>
      </footer>
    </>
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);

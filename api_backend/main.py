"use client";

import { useEffect, useState } from "react";
import { apiPost } from "@/app/lib/api";
import { motion } from "framer-motion";

import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    Tooltip,
    CartesianGrid,
    Area,
    Legend,
    ResponsiveContainer
} from "recharts";

interface HistoryItem {
    date: string;
    value: number;
}

interface ForecastItem {
    date: string;
    value: number;
    lower: number;
    upper: number;
}

interface ForecastResponse {
    history: HistoryItem[];
    forecast: ForecastItem[];
}

interface CombinedRow {
    date: string;
    history: number | null;
    forecast: number | null;
    lower: number | null;
    upper: number | null;
}

export default function ForecastChart() {
    const [data, setData] = useState<CombinedRow[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        async function load() {
            try {
                setError(null);

                // 🚨 agora correto: NÃO usar "/api/forecast"
                const res: ForecastResponse = await apiPost("/forecast", {
                    metric: "visitors",
                    periods: 30
                });

                if (!res?.history || !res?.forecast) {
                    setError("Resposta inesperada do servidor.");
                    setLoading(false);
                    return;
                }

                const combined: CombinedRow[] = [
                    ...res.history.map(h => ({
                        date: h.date,
                        history: h.value,
                        forecast: null,
                        lower: null,
                        upper: null
                    })),
                    ...res.forecast.map(f => ({
                        date: f.date,
                        history: null,
                        forecast: f.value,
                        lower: f.lower,
                        upper: f.upper
                    }))
                ];

                setData(combined);
            } catch (err) {
                console.error("Erro ao carregar previsão:", err);
                setError("Não foi possível carregar a previsão.");
            } finally {
                setLoading(false);
            }
        }

        load();
    }, []);

    // --- Estados de carregamento ou erro ---
    if (loading) return <p>Carregando previsão...</p>;
    if (error) return <p className="text-red-500">{error}</p>;
    if (!data.length) return <p>Nenhum dado disponível.</p>;

    return (
        <motion.div
            initial={{ opacity: 0, scale: 0.97 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.6 }}
            className="bg-white p-4 rounded-xl shadow-md"
        >
            <motion.h2
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 }}
                className="text-xl font-bold mb-4"
            >
                Previsão de Visitantes (30 dias)
            </motion.h2>

            <ResponsiveContainer width="100%" height={400}>
                <LineChart data={data}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis />

                    <Tooltip />
                    <Legend />

                    {/* Faixa de incerteza */}
                    <Area
                        type="monotone"
                        dataKey="upper"
                        stroke="none"
                        fillOpacity={0.15}
                        fill="#999"
                    />

                    {/* Linha histórica */}
                    <Line
                        type="monotone"
                        dataKey="history"
                        stroke="#3b82f6"
                        strokeWidth={2}
                        dot={false}
                    />

                    {/* Linha forecast */}
                    <Line
                        type="monotone"
                        dataKey="forecast"
                        stroke="#16a34a"
                        strokeWidth={2}
                        strokeDasharray="5 5"
                        dot={false}
                    />
                </LineChart>
            </ResponsiveContainer>
        </motion.div>
    );
}

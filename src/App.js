import React, { useState, useEffect, useCallback } from 'react';
import {
    LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip, Legend, CartesianGrid, ResponsiveContainer
} from 'recharts';
import {
    Sun, Moon, Thermometer, Flame, Fan, Droplet, Gauge, Clock, CalendarDays, AlertTriangle
} from 'lucide-react';

// Define conversion constants
const kgPerHour = 37;
const feedTime = kgPerHour / 3600;

// Function to fetch data with exponential backoff for retries
const fetchDataWithRetry = async (url) => {
    const MAX_RETRIES = 3;
    const BASE_DELAY_MS = 1000;
    for (let i = 0; i < MAX_RETRIES; i++) {
        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Attempt ${i + 1} failed for ${url}:`, error);
            if (i < MAX_RETRIES - 1) {
                await new Promise(res => setTimeout(res, BASE_DELAY_MS * Math.pow(2, i)));
            } else {
                return null;
            }
        }
    }
};

// Main App component
const App = () => {
    // State hooks for data and UI state
    const [currentInfo, setCurrentInfo] = useState(null);
    const [statsData, setStatsData] = useState([]);
    const [consumptionData, setConsumptionData] = useState([]);
    const [monthlyConsumptionData, setMonthlyConsumptionData] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [theme, setTheme] = useState(() => localStorage.getItem('theme') || 'dark');
    const [apiError, setApiError] = useState(false);
    const [errorMessage, setErrorMessage] = useState('');

    // State for the selectable time range in hours, with 24 hours as the default
    const [timeRange, setTimeRange] = useState(24);

    const [lineVisibility, setLineVisibility] = useState({
        tSet: true,
        tBoiler: true,
        tRetWater: true,
        dhw: true,
        tFlueGases: true,
        tOutside: true,
        power: true,
        flame: true,
    });

    const toggleTheme = () => {
        setTheme(prevTheme => (prevTheme === 'light' ? 'dark' : 'light'));
    };

    useEffect(() => {
        const root = window.document.documentElement;
        if (theme === 'dark') {
            root.classList.add('dark');
        } else {
            root.classList.remove('dark');
        }
        localStorage.setItem('theme', theme);
    }, [theme]);

    const fetchAllData = useCallback(async () => {
        try {
            setApiError(false);
            setErrorMessage('');

            const timestampAgo = Math.floor(Date.now() / 1000) - (timeRange * 60 * 60);

            const [
                info,
                stats,
                consumption,
                monthlyConsumption
            ] = await Promise.all([
                fetchDataWithRetry('api/getInfo'),
                fetchDataWithRetry(`api/getStats?timestamp=${timestampAgo}`),
                fetchDataWithRetry(`api/getConsumptionStats?timestamp=${timestampAgo}`),
                fetchDataWithRetry('api/getConsumptionByMonth')
            ]);
            
            if (info && info.length > 0) {
                setCurrentInfo(info[0]);
            }
            
            // CORRECTED: Using 'en-GB' locale for Day/Month date format
            const localeOptions = {
                day: timeRange > 24 ? 'numeric' : undefined,
                month: timeRange > 24 ? 'numeric' : undefined,
                hour: '2-digit',
                minute: '2-digit',
                hour12: false,
            };

            const mappedStats = stats?.map(item => ({
                date: new Date(item.Date).getTime(),
                formattedDate: new Date(item.Date).toLocaleString('en-GB', localeOptions),
                tSet: item.Tset,
                tBoiler: item.Tboiler,
                tRetWater: item.TDS18,
                dhw: item.DHW,
                tFlueGases: item.KTYPE,
                tOutside: item.TBMP,
                flame: item.Flame,
                // CORRECTED: Ensure power is never negative. (0=Off, 1=Suspend -> 0 on graph)
                power: Math.max(0, item.Power - 1),
            })) || [];
            
            const mappedConsumption = consumption?.map(item => ({
                date: new Date(item.Timestamp).getTime(),
                formattedDate: new Date(item.Timestamp).toLocaleString('en-GB', localeOptions),
                consumption: Math.round(item.FFWorkTime * feedTime * 100) / 100
            })) || [];
            
            const mappedMonthly = monthlyConsumption?.map(item => {
                if (!item.yr_mon || item.FFWork === undefined) return null;
                try {
                    const date = new Date(item.yr_mon + '-01T12:00:00Z');
                    return {
                        formattedDate: date.toLocaleString('default', { month: 'short', year: 'numeric' }),
                        consumption: Math.round(item.FFWork * feedTime)
                    };
                } catch (e) {
                    return null;
                }
            }).filter(Boolean) || [];

            setStatsData(mappedStats);
            setConsumptionData(mappedConsumption);
            setMonthlyConsumptionData(mappedMonthly);

        } catch (error) {
            console.error("Error fetching data:", error);
            setApiError(true);
            setErrorMessage("Failed to fetch data from the API. Please check your network connection or try again later.");
        } finally {
            setIsLoading(false);
        }
    }, [timeRange]);

    useEffect(() => {
        setIsLoading(true);
        fetchAllData();
        const interval = setInterval(fetchAllData, 30000);
        return () => clearInterval(interval);
    }, [fetchAllData]);

    const handleLegendClick = (e) => {
        const { dataKey } = e;
        setLineVisibility(prevState => ({
            ...prevState,
            [dataKey]: !prevState[dataKey],
        }));
    };

    const renderLegendText = (value, entry) => {
        const { dataKey } = entry;
        const isActive = lineVisibility[dataKey];
        const color = isActive ? (theme === 'dark' ? '#fff' : '#333') : '#999';
        return <span style={{ color }}>{value}</span>;
    };

    const StatusCard = ({ label, value, icon: Icon }) => (
        <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-md flex items-center justify-between">
            <div className="flex items-center">
                <Icon className="text-blue-500 dark:text-blue-400 mr-3 w-6 h-6" />
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    {label}: <span className="text-lg font-bold text-gray-900 dark:text-white">{value}</span>
                </span>
            </div>
        </div>
    );
    
    const timeRangeLabel = {
        1: '[last hour]',
        6: '[last 6 hours]',
        12: '[last 12 hours]',
        24: '[last 24 hours]',
        48: '[last 2 days]',
        72: '[last 3 days]',
    }[timeRange];
    
    const TimeRangeSelector = () => {
        const ranges = [
            { label: '1 Hour', value: 1 },
            { label: '6 Hours', value: 6 },
            { label: '12 Hours', value: 12 },
            { label: '24 Hours', value: 24 },
            { label: '2 Days', value: 48 },
            { label: '3 Days', value: 72 },
        ];
        return (
            <div className="grid grid-cols-3 sm:flex items-center gap-2 mb-6 bg-gray-200 dark:bg-gray-800/50 p-1 rounded-lg">
                {ranges.map(range => (
                    <button
                        key={range.value}
                        onClick={() => setTimeRange(range.value)}
                        className={`px-3 py-2 text-sm font-semibold rounded-md transition-colors w-full ${
                            timeRange === range.value
                                ? 'bg-blue-500 text-white shadow-md'
                                : 'text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-700'
                        }`}
                    >
                        {range.label}
                    </button>
                ))}
            </div>
        );
    };
    
    if (isLoading) {
        return (
            <div className={`min-h-screen p-4 flex items-center justify-center transition-colors duration-500 bg-gray-100 dark:bg-gray-900 text-white`}>
                <div className="text-xl font-semibold">Loading boiler data...</div>
            </div>
        );
    }
    
    const statusCards = currentInfo ? [
        <StatusCard key="mode" label="Mode" value={currentInfo.Mode === 0 ? 'Standby' : currentInfo.Mode === 1 ? 'Auto' : 'Timer'} icon={Clock} />,
        <StatusCard key="state" label="State" value={currentInfo.State === 0 ? 'CH Priority' : currentInfo.State === 1 ? 'DHW Priority' : currentInfo.State === 2 ? 'Parallel Pumps' : 'Summer Mode'} icon={Gauge} />,
        <StatusCard key="status" label="Status" value={(currentInfo.Status === 0 ? 'Idle' : currentInfo.Status === 1 ? 'Fan Cleaning' : currentInfo.Status === 2 ? 'Cleaner' : currentInfo.Status === 3 ? 'Wait' : currentInfo.Status === 4 ? 'Loading' : currentInfo.Status === 5 ? 'Heating' : currentInfo.Status === 6 ? 'Ignition1' : currentInfo.Status === 7 ? 'Ignition2' : currentInfo.Status === 8 ? 'Unfolding' : currentInfo.Status === 9 ? 'Burning' : currentInfo.Status === 10 ? 'Extinction' : 'Standby/Extinct') + (currentInfo.Power > 0 ? ` / P${currentInfo.Power}` : '')} icon={Flame} />,
        <StatusCard key="flame" label="Flame" value={`${currentInfo.Flame} lx`} icon={Flame} />,
        <StatusCard key="fan" label="Fan %" value={`${currentInfo.Fan} %`} icon={Fan} />,
        <StatusCard key="tboiler" label="TBoiler" value={`${currentInfo.Tboiler} °C`} icon={Thermometer} />,
        <StatusCard key="tdhw" label="TDHW" value={`${currentInfo.DHW} °C`} icon={Thermometer} />,
        <StatusCard key="toutside" label="Toutside" value={`${currentInfo.TBMP} °C`} icon={Thermometer} />,
        <StatusCard key="chpump" label="CH Pump" value={currentInfo.CHPump === false ? 'Off' : 'On'} icon={Droplet} />,
        <StatusCard key="dhwpump" label="DHW Pump" value={currentInfo.DHWPump === false ? 'Off' : 'On'} icon={Droplet} />,
        <StatusCard key="consumption" label="Consumption" value={`${consumptionData.length > 0 ? consumptionData[consumptionData.length - 1].consumption.toFixed(2) : '0.00'} kg / 24h`} icon={CalendarDays} />
    ] : [];

    return (
        <div className="min-h-screen p-4 sm:p-6 transition-colors duration-500 font-sans bg-gray-100 text-gray-900 dark:bg-gray-900 dark:text-white">
            <header className="flex justify-between items-center mb-6">
                <h1 className="text-3xl font-bold text-gray-900 dark:text-white">NPBC-Monitor</h1>
                <button
                    onClick={toggleTheme}
                    className="p-2 rounded-full bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-white transition-transform duration-300 hover:scale-110"
                >
                    {theme === 'light' ? <Moon size={24} /> : <Sun size={24} />}
                </button>
            </header>
            
            <TimeRangeSelector />
            
            {apiError && (
                <div className="flex items-center p-4 mb-4 text-sm text-red-800 rounded-lg bg-red-50 dark:bg-gray-800 dark:text-red-400" role="alert">
                    <AlertTriangle className="w-5 h-5 mr-3" />
                    <span className="sr-only">Danger</span>
                    <div>
                        <span className="font-medium">Error: </span> {errorMessage}
                    </div>
                </div>
            )}
            <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                <div className="lg:col-span-1">
                    <div className="bg-white dark:bg-gray-800/50 p-6 rounded-2xl shadow-lg">
                        <h2 className="text-xl font-semibold text-gray-800 dark:text-white mb-4">Current State</h2>
                        <div className="space-y-4">{statusCards}</div>
                    </div>
                </div>
                <div className="lg:col-span-3 space-y-6">
                    <div className="bg-white dark:bg-gray-800/50 p-6 rounded-2xl shadow-lg">
                        <h2 className="text-xl font-semibold text-gray-800 dark:text-white mb-4">Temperature {timeRangeLabel}</h2>
                        <div className="w-full h-80">
                            {statsData.length > 0 ? (
                                <ResponsiveContainer width="100%" height="100%">
                                    <LineChart data={statsData}>
                                        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.3} />
                                        <XAxis dataKey="formattedDate" stroke={theme === 'dark' ? '#aaa' : '#555'} tick={{ fontSize: 12 }} interval="auto" />
                                        <YAxis label={{ value: 'Temp (°C)', angle: -90, position: 'insideLeft', fill: theme === 'dark' ? '#aaa' : '#555' }} stroke={theme === 'dark' ? '#aaa' : '#555'} />
                                        <Tooltip contentStyle={{ backgroundColor: theme === 'dark' ? '#27272a' : '#ffffff' }} />
                                        <Legend onClick={handleLegendClick} formatter={renderLegendText} />
                                        <Line type="monotone" dataKey="tSet" name="Tset" stroke="#a78bfa" dot={false} hide={!lineVisibility.tSet} />
                                        <Line type="monotone" dataKey="tBoiler" name="Tboiler(out)" stroke="#ef4444" dot={false} hide={!lineVisibility.tBoiler} />
                                        <Line type="monotone" dataKey="tRetWater" name="Tboiler(in)" stroke="#3b82f6" dot={false} hide={!lineVisibility.tRetWater} />
                                        <Line type="monotone" dataKey="dhw" name="DHW" stroke="#06b6d4" dot={false} hide={!lineVisibility.dhw} />
                                        {/* CORRECTED: Changed stroke color for better visibility */}
                                        <Line type="monotone" dataKey="tFlueGases" name="TFlueGases" stroke="#6c7d52" dot={false} hide={!lineVisibility.tFlueGases} />
                                        <Line type="monotone" dataKey="tOutside" name="Toutside" stroke="#eab308" dot={false} hide={!lineVisibility.tOutside} />
                                    </LineChart>
                                </ResponsiveContainer>
                            ) : (
                                <div className="flex items-center justify-center h-full text-gray-500">No temperature data available.</div>
                            )}
                        </div>
                    </div>
                    <div className="bg-white dark:bg-gray-800/50 p-6 rounded-2xl shadow-lg">
                        <h2 className="text-xl font-semibold text-gray-800 dark:text-white mb-4">Flame and Power {timeRangeLabel}</h2>
                         <div className="w-full h-80">
                            {statsData.length > 0 ? (
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={statsData}>
                                        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.3} />
                                        <XAxis dataKey="formattedDate" stroke={theme === 'dark' ? '#aaa' : '#555'} tick={{ fontSize: 12 }} interval="auto" />
                                        <YAxis yAxisId="left" label={{ value: 'Flame (lx)', angle: -90, position: 'insideLeft', fill: theme === 'dark' ? '#aaa' : '#555' }} stroke={theme === 'dark' ? '#aaa' : '#555'} />
                                        <YAxis yAxisId="right" orientation="right" label={{ value: 'Power', angle: 90, position: 'insideRight', fill: theme === 'dark' ? '#aaa' : '#555' }} stroke={theme === 'dark' ? '#aaa' : '#555'} domain={[0, 6]} allowDataOverflow={true} />
                                        <Tooltip contentStyle={{ backgroundColor: theme === 'dark' ? '#27272a' : '#ffffff' }}/>
                                        <Legend onClick={handleLegendClick} formatter={renderLegendText} />
                                        <Bar yAxisId="right" dataKey="power" name="Power" fill="#c084fc" hide={!lineVisibility.power} />
                                        <Line yAxisId="left" type="monotone" dataKey="flame" name="Flame" stroke="#f97316" dot={false} strokeWidth={2} hide={!lineVisibility.flame} />
                                    </BarChart>
                                </ResponsiveContainer>
                            ) : (
                                <div className="flex items-center justify-center h-full text-gray-500">No flame or power data available.</div>
                            )}
                        </div>
                    </div>
                    <div className="bg-white dark:bg-gray-800/50 p-6 rounded-2xl shadow-lg">
                        <h2 className="text-xl font-semibold text-gray-800 dark:text-white mb-4">Consumption {timeRangeLabel}</h2>
                        <div className="w-full h-64">
                            {consumptionData.length > 0 ? (
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={consumptionData}>
                                        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.3} />
                                        <XAxis dataKey="formattedDate" stroke={theme === 'dark' ? '#aaa' : '#555'} tick={{ fontSize: 12 }} interval="auto" />
                                        <YAxis label={{ value: 'Consumption (kg)', angle: -90, position: 'insideLeft', fill: theme === 'dark' ? '#aaa' : '#555' }} stroke={theme === 'dark' ? '#aaa' : '#555'} />
                                        <Tooltip contentStyle={{ backgroundColor: theme === 'dark' ? '#27272a' : '#ffffff' }} formatter={(value) => `${value} kg`} />
                                        <Legend />
                                        <Bar dataKey="consumption" name="Consumption (kg)" fill="#d97706" />
                                    </BarChart>
                                </ResponsiveContainer>
                            ) : (
                                 <div className="flex items-center justify-center h-full text-gray-500">No consumption data available for this period.</div>
                            )}
                        </div>
                    </div>
                    <div className="bg-white dark:bg-gray-800/50 p-6 rounded-2xl shadow-lg">
                        <h2 className="text-xl font-semibold text-gray-800 dark:text-white mb-4">Monthly Consumption</h2>
                        <div className="w-full h-64">
                            {monthlyConsumptionData.length > 0 ? (
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={monthlyConsumptionData}>
                                        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.3} />
                                        <XAxis dataKey="formattedDate" stroke={theme === 'dark' ? '#aaa' : '#555'} />
                                        <YAxis label={{ value: 'Consumption (kg)', angle: -90, position: 'insideLeft', fill: theme === 'dark' ? '#aaa' : '#555' }} stroke={theme === 'dark' ? '#aaa' : '#555'} />
                                        <Tooltip contentStyle={{ backgroundColor: theme === 'dark' ? '#27272a' : '#ffffff' }} formatter={(value) => `${value} kg`} />
                                        <Legend />
                                        <Bar dataKey="consumption" name="Monthly Consumption (kg)" fill="#d97706" />
                                    </BarChart>
                                </ResponsiveContainer>
                            ) : (
                                <div className="flex items-center justify-center h-full text-gray-500">No monthly consumption data available.</div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default App;

/**
 * AI Automated Workflow — single-file demo server (TypeScript)
 * ------------------------------------------------------------
 * Features:
 * - Chat pop-up ingestion → AI-ish parser (regex) → order draft → create order
 * - Vendor→Driver linking, auto-assignment (scoring), OTP delivery completion
 * - Reporting (total deliveries, success rate, COD ratio, avg delivery time, revenue)
 * - Socket.IO namespaces: /merchant (events), /driver (job.assigned)
 *
 * NOTE: This uses in-memory stores for fast testing. Replace with your DB later.
 *
 * Quick run:
 *   npm init -y
 *   npm i express socket.io cors uuid
 *   npm i -D typescript ts-node @types/node @types/express @types/cors
 *   npx tsc --init   # set "esModuleInterop": true
 *   npx ts-node ai-workflow.ts
 */

import express from 'express';
import http from 'http';
import cors from 'cors';
import { Server as IOServer } from 'socket.io';
import { randomUUID } from 'crypto';
import { v4 as uuidv4 } from 'uuid';

// --------------------------- Types & Helpers ---------------------------

type ID = string;
type PaymentMethod = 'COD' | 'PREPAID';
type OrderStatus = 'created'|'assigned'|'enroute'|'delivered'|'failed'|'cancelled';

interface Vendor { vendor_id: ID; name: string; kpi_score: number; }
interface Driver { driver_id: ID; vendor_id: ID; name: string; phone: string; status: 'active'|'off'|'busy'; }
interface Merchant { merchant_id: ID; name: string; vendor_preference?: ID; }
interface Customer { customer_id: ID; merchant_id: ID; name: string; phone: string; address_text?: string; lat?: number; lng?: number; }
interface OrderItem { id: number; order_id: ID; sku?: string; name: string; qty: number; price_cents?: number; }
interface Order {
  order_id: ID; merchant_id: ID; vendor_id?: ID; customer_id: ID;
  payment_method: PaymentMethod; price_cents?: number; status: OrderStatus;
  address_text?: string; lat?: number; lng?: number; delivery_window?: string;
  notes?: string; tracking_url?: string; created_at: number; delivered_at?: number;
}
interface Assignment { assignment_id: ID; order_id: ID; vendor_id: ID; driver_id: ID; assigned_at: number; }
interface DeliveryEvent { id: number; order_id: ID; event: string; at: number; meta?: any; }
interface ChatTranscript { id: number; order_id?: ID; thread_id: string; transcript: any[]; created_at: number; }

const now = () => Date.now();
const maskPhone = (p: string) => p.replace(/^(\+\d{2})\d+(\d{2})$/, '$1****$2');
const genOtp = () => String(Math.floor(100000 + Math.random() * 900000));
const rnd = (len=8) => [...cryptoRandom(len)].join('');
function* cryptoRandom(len: number) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  for (let i=0;i<len;i++) yield chars[Math.floor(Math.random()*chars.length)];
}

// --------------------------- In-memory Stores ---------------------------

const Vendors = new Map<ID, Vendor>();
const Drivers = new Map<ID, Driver>();
const Merchants = new Map<ID, Merchant>();
const Customers = new Map<ID, Customer>();
const Orders = new Map<ID, Order>();
const OrderItems = new Map<number, OrderItem>();
const Assignments = new Map<ID, Assignment>();
const Events: DeliveryEvent[] = [];
const Chats: ChatTranscript[] = [];

let orderItemSeq = 1;
let deliveryEventSeq = 1;
let chatSeq = 1;

const OTPStore = new Map<ID, { otp: string; exp: number }>();

// --------------------------- Socket.IO Setup ---------------------------

const app = express();
app.use(cors());
app.use(express.json());
const server = http.createServer(app);
const io = new IOServer(server, { cors: { origin: '*' } });

const nsMerchant = io.of('/merchant');
const nsDriver = io.of('/driver');

// Driver joins a room named by driver_id; merchant joins by merchant_id
nsDriver.on('connection', (socket) => {
  socket.on('join', (driver_id: string) => socket.join(driver_id));
});
nsMerchant.on('connection', (socket) => {
  socket.on('join', (merchant_id: string) => socket.join(merchant_id));
});

function emitMerchant(merchant_id: ID, event: string, payload: any) {
  nsMerchant.to(merchant_id).emit(event, payload);
}
function notifyDriver(driver_id: ID, payload: any) {
  nsDriver.to(driver_id).emit('job.assigned', payload);
}

// --------------------------- AI-ish Chat Parser ---------------------------

const phoneRe = /(\+?\d{8,15})/;
const mapsRe = /(https?:\/\/(?:www\.)?google\.[^ ]+\/maps[^\s]+)/i;
// "Product x 2 @ 30" or "Product * 2" etc.
const itemLineRe = /(?:^|\n|\r)\s*([^\-\n\r:@][^:\n\r]*?)\s*(?:x|X|\*|×)\s*(\d+)(?:\s*@\s*(\d+))?/g;

function parseChat(message: string) {
  const out: any = {};
  const phoneM = message.match(phoneRe);
  if (phoneM) out.phone = phoneM[1];

  const mapsM = message.match(mapsRe);
  if (mapsM) out.map_link = mapsM[1];

  if (!out.customer_name) {
    const idx = phoneM ? message.indexOf(phoneM[0]) : -1;
    out.customer_name = idx > 0 ? message.slice(0, idx).trim().split(/\n/)[0].slice(0, 60) : undefined;
  }

  const items: any[] = [];
  let m: RegExpExecArray | null;
  while ((m = itemLineRe.exec(message)) !== null) {
    items.push({ name: m[1].trim(), qty: parseInt(m[2],10), price_cents: m[3] ? parseInt(m[3],10)*100 : undefined });
  }
  if (items.length) out.items = items;

  out.payment_method = /cod|cash on delivery/i.test(message) ? 'COD' :
                       /prepaid|online|link/i.test(message) ? 'PREPAID' : undefined;

  return out;
}

function askText(locale: 'ar'|'en', fields: string[]) {
  const map: Record<string,string> = { customer_name:'name', phone:'phone', items:'items/qty' };
  const pretty = fields.map(f=>map[f]||f).join(', ');
  return locale === 'ar'
    ? `لإتمام الطلب أحتاج: ${fields.map(f => f==='customer_name'?'الاسم':f==='phone'?'الجوال':f==='items'?'المنتجات والكمية':f).join(', ')}`
    : `To place the order I need: ${pretty}.`;
}

// --------------------------- Seed (optional) ---------------------------

function seed() {
  if (Vendors.size) return; // one-time
  const vendorA: Vendor = { vendor_id: uuidv4(), name: 'FastWings', kpi_score: 0.7 };
  const vendorB: Vendor = { vendor_id: uuidv4(), name: 'GulfExpress', kpi_score: 0.85 };
  Vendors.set(vendorA.vendor_id, vendorA);
  Vendors.set(vendorB.vendor_id, vendorB);

  const driver1: Driver = { driver_id: uuidv4(), vendor_id: vendorA.vendor_id, name: 'Omar', phone: '+966500000001', status: 'active' };
  const driver2: Driver = { driver_id: uuidv4(), vendor_id: vendorB.vendor_id, name: 'Sara', phone: '+966500000002', status: 'active' };
  Drivers.set(driver1.driver_id, driver1);
  Drivers.set(driver2.driver_id, driver2);

  const merchant: Merchant = { merchant_id: uuidv4(), name: 'Al-Nour Market', vendor_preference: vendorB.vendor_id };
  Merchants.set(merchant.merchant_id, merchant);

  const customer: Customer = { customer_id: uuidv4(), merchant_id: merchant.merchant_id, name: 'Ahmed Ali', phone: '+966512345678' };
  Customers.set(customer.customer_id, customer);

  console.log('SEED:',
    { merchant, vendors: [vendorA, vendorB], drivers: [driver1, driver2], customer });
}
seed();

// --------------------------- REST Endpoints ---------------------------

// Chat ingest → parse → ask/confirm
app.post('/v1/chat/ingest', (req, res) => {
  const { merchant_id, thread_id, locale = 'en', channel = 'webchat', message } = req.body || {};
  const merchant = Merchants.get(merchant_id);
  if (!merchant) return res.status(400).json({ error: 'merchant not found' });
  if (!message) return res.status(400).json({ error: 'message required' });

  const parsed = parseChat(String(message));
  const missing: string[] = [];
  if (!parsed.customer_name) missing.push('customer_name');
  if (!parsed.phone) missing.push('phone');
  if (!parsed.items?.length) missing.push('items');

  if (missing.length) {
    return res.json({ mode: 'ASK', ask: askText(locale, missing), missing });
  }

  // Upsert customer
  let customer = [...Customers.values()].find(c => c.merchant_id === merchant_id && c.phone === parsed.phone);
  if (!customer) {
    customer = { customer_id: uuidv4(), merchant_id, name: parsed.customer_name, phone: parsed.phone };
    Customers.set(customer.customer_id, customer);
  }

  // Draft (price/sku validation could be added here)
  const draft = {
    merchant_id,
    customer_id: customer.customer_id,
    items: parsed.items,
    address_text: parsed.address_text,
    lat: parsed.lat, lng: parsed.lng,
    payment_method: parsed.payment_method || 'COD',
    notes: parsed.notes,
    delivery_window: parsed.delivery_window
  };

  // Store transient transcript record (unattached draft)
  Chats.push({ id: chatSeq++, thread_id, transcript: [{ ts: now(), sender:'customer', text: message }], created_at: now() });

  return res.json({ mode: 'CONFIRM', draft });
});

// Create order
app.post('/v1/orders', (req, res) => {
  try {
    const dto = req.body || {};
    const merchant = Merchants.get(dto.merchant_id);
    const customer = Customers.get(dto.customer_id);
    if (!merchant || !customer) return res.status(400).json({ error: 'merchant or customer not found' });
    if (!Array.isArray(dto.items) || !dto.items.length) return res.status(400).json({ error: 'items required' });
    if (!['COD','PREPAID'].includes(dto.payment_method)) return res.status(400).json({ error: 'payment_method invalid' });

    const order_id = uuidv4();
    const order: Order = {
      order_id,
      merchant_id: dto.merchant_id,
      customer_id: dto.customer_id,
      payment_method: dto.payment_method,
      price_cents: dto.price_cents,
      status: 'created',
      address_text: dto.address_text,
      lat: dto.lat, lng: dto.lng,
      delivery_window: dto.delivery_window,
      notes: dto.notes,
      tracking_url: `https://track.example.com/${rnd(10)}`,
      created_at: now()
    };
    Orders.set(order_id, order);

    for (const it of dto.items) {
      const item: OrderItem = {
        id: orderItemSeq++,
        order_id,
        sku: it.sku,
        name: it.name,
        qty: Number(it.qty || 1),
        price_cents: it.price_cents
      };
      OrderItems.set(item.id, item);
    }

    Events.push({ id: deliveryEventSeq++, order_id, event: 'created', at: now(), meta: {} });
    emitMerchant(order.merchant_id, 'order.created', { order_id, tracking_url: order.tracking_url });

    return res.json({ order_id, tracking_url: order.tracking_url });
  } catch (e:any) {
    return res.status(500).json({ error: e.message || 'failed' });
  }
});

// Chat helper: create order directly from a CONFIRM draft
app.post('/v1/chat/create-order', (req, res) => {
  // reuse /v1/orders logic
  (app._router as any).handle({ ...req, url: '/v1/orders', method: 'POST' }, res, () => {});
});

// Plan assignment (choose vendor + driver candidates)
app.post('/v1/assignment/plan', (req, res) => {
  const { order_id } = req.body || {};
  const order = Orders.get(order_id);
  if (!order) return res.status(404).json({ error: 'order not found' });

  // Simple scoring: priceRank (random), KPI, capacity
  const vendors = [...Vendors.values()];
  if (!vendors.length) return res.status(400).json({ error: 'no vendors' });

  const candidates = vendors.map(v => {
    const activeDrivers = [...Drivers.values()].filter(d => d.vendor_id === v.vendor_id && d.status === 'active').length;
    const capacityOk = activeDrivers > 0 ? 1 : 0.3;
    const kpi = v.kpi_score || 0.5;
    const priceRank = Math.random(); // stub: replace with your pricing engine
    const coverage = 1;              // stub: inside coverage
    const score = 0.35*priceRank + 0.35*kpi + 0.20*coverage + 0.10*capacityOk;
    return { vendor_id: v.vendor_id, score_breakdown: { priceRank, kpi, coverage, capacityOk }, score };
  }).sort((a,b)=> b.score - a.score);

  const top = candidates[0];
  const driverCandidate = [...Drivers.values()].find(d => d.vendor_id === top.vendor_id && d.status === 'active');
  const driver_candidates = driverCandidate ? [{ driver_id: driverCandidate.driver_id, eta: 20, capacity_ok: true }] : [];

  return res.json({ vendor_choice: top, driver_candidates });
});

// Assign a driver (sets vendor attribution, emits sockets)
app.post('/v1/assignments', (req, res) => {
  const { order_id, driver_id } = req.body || {};
  const order = Orders.get(order_id);
  if (!order) return res.status(404).json({ error: 'order not found' });

  const driver = Drivers.get(driver_id);
  if (!driver) return res.status(404).json({ error: 'driver not found' });

  order.vendor_id = driver.vendor_id;
  order.status = 'assigned';
  Orders.set(order_id, order);

  const assignment: Assignment = {
    assignment_id: uuidv4(),
    order_id,
    vendor_id: driver.vendor_id,
    driver_id: driver.driver_id,
    assigned_at: now()
  };
  Assignments.set(assignment.assignment_id, assignment);

  Events.push({ id: deliveryEventSeq++, order_id, event: 'assigned', at: now(), meta: { driver_id } });

  emitMerchant(order.merchant_id, 'order.assigned', { order_id, driver_id });
  notifyDriver(driver_id, { order_id, pickup: { merchant_id: order.merchant_id }, dropoff: { customer_id: order.customer_id }, otp_required: true });

  return res.json({ assignment_id: assignment.assignment_id, vendor_id: driver.vendor_id, driver_id: driver.driver_id });
});

// Payment link (stub)
app.post('/v1/orders/:id/payment-link', (req, res) => {
  const order = Orders.get(req.params.id);
  if (!order) return res.status(404).json({ error: 'order not found' });
  const url = `https://pay.example.com/${order.order_id}`;
  return res.json({ url });
});

// Issue OTP (test helper) — not exposed in earlier spec, but useful to simulate SMS step
app.post('/v1/orders/:id/issue-otp', (req, res) => {
  const order = Orders.get(req.params.id);
  if (!order) return res.status(404).json({ error: 'order not found' });
  const otp = genOtp();
  OTPStore.set(order.order_id, { otp, exp: now() + 10 * 60 * 1000 }); // 10 min
  return res.json({ otp }); // in production, send via SMS/WhatsApp instead
});

// Complete order with OTP
app.post('/v1/orders/:id/complete', (req, res) => {
  const order = Orders.get(req.params.id);
  if (!order) return res.status(404).json({ error: 'order not found' });

  const { otp } = req.body || {};
  const rec = OTPStore.get(order.order_id);
  if (!rec || rec.exp < now() || rec.otp !== otp) return res.status(400).json({ error: 'invalid or expired OTP' });

  order.status = 'delivered';
  order.delivered_at = now();
  Orders.set(order.order_id, order);
  OTPStore.delete(order.order_id);

  Events.push({ id: deliveryEventSeq++, order_id: order.order_id, event: 'delivered', at: now(), meta: {} });
  emitMerchant(order.merchant_id, 'order.delivered', { order_id: order.order_id });

  return res.json({ ok: true });
});

// Vendor: drivers list
app.get('/v1/vendors/:id/drivers', (req, res) => {
  const vendor_id = req.params.id;
  const drivers = [...Drivers.values()].filter(d => d.vendor_id === vendor_id);
  return res.json(drivers);
});

// Vendor: reports
app.get('/v1/vendors/:id/reports', (req, res) => {
  const vendor_id = req.params.id;
  const orders = [...Orders.values()].filter(o => o.vendor_id === vendor_id);
  const totalDelivered = orders.filter(o => o.status === 'delivered').length;
  const successRate = orders.length ? totalDelivered / orders.length : 0;
  const codCount = orders.filter(o => o.payment_method === 'COD').length;
  const codRatio = orders.length ? codCount / orders.length : 0;
  const delivered = orders.filter(o => o.delivered_at);
  const avgDeliveryMinutes = delivered.length
    ? delivered.reduce((sum, o) => sum + ((o.delivered_at! - o.created_at) / 60000), 0) / delivered.length
    : 0;
  const revenue = orders.filter(o => o.status === 'delivered').reduce((s, o) => s + (o.price_cents || 0), 0);

  return res.json({
    total_deliveries: totalDelivered,
    success_rate: successRate,
    cod_ratio: codRatio,
    avg_delivery_minutes: avgDeliveryMinutes,
    revenue_cents: revenue
  });
});

// Simple health
app.get('/health', (_req, res) => res.json({ ok: true }));

// --------------------------- Start Server ---------------------------

const PORT = Number(process.env.PORT || 3000);
server.listen(PORT, () => {
  console.log(`AI Workflow API running on http://localhost:${PORT}`);
  console.log(`Socket.IO namespaces: /merchant and /driver`);
  console.log(`Try: POST /v1/chat/ingest then /v1/chat/create-order`);
});

/**
 * CURL QUICK TESTS
 * ----------------
 * 1) Find the seeded merchant_id and a driver_id in the console output (or add endpoints to list them).
 *    For quick demo: copy values printed in SEED above by adding console.log there if needed.
 *
 * 2) Chat → draft:
 * curl -X POST http://localhost:3000/v1/chat/ingest -H "Content-Type: application/json" -d '{
 *   "merchant_id":"<MERCHANT_ID>",
 *   "thread_id":"t1",
 *   "locale":"en",
 *   "channel":"webchat",
 *   "message":"Ahmed Ali +966512345678 https://maps.google.com/?q=24.7136,46.6753 Shampoo x 2 @ 30, Soap x 1 @ 10 COD"
 * }'
 *
 * 3) Create order:
 * curl -X POST http://localhost:3000/v1/chat/create-order -H "Content-Type: application/json" -d '{ "merchant_id":"...", "customer_id":"...", "items":[{"name":"Shampoo","qty":2,"price_cents":3000},{"name":"Soap","qty":1,"price_cents":1000}], "payment_method":"COD" }'
 *
 * 4) Plan + Assign:
 * curl -X POST http://localhost:3000/v1/assignment/plan -H "Content-Type: application/json" -d '{"order_id":"<ORDER_ID>"}'
 * curl -X POST http://localhost:3000/v1/assignments -H "Content-Type: application/json" -d '{"order_id":"<ORDER_ID>","driver_id":"<DRIVER_ID>"}'
 *
 * 5) OTP flow:
 * curl -X POST http://localhost:3000/v1/orders/<ORDER_ID>/issue-otp -H "Content-Type: application/json" -d '{}'
 * curl -X POST http://localhost:3000/v1/orders/<ORDER_ID>/complete -H "Content-Type: application/json" -d '{"otp":"123456"}'
 */

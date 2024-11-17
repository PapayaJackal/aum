import {env} from '$env/dynamic/private';
import type { Actions } from './$types';
let apiUrl = env.API_URL || 'http://localhost:8000'

export const actions = {
  default: async (event) => {
    let fd = await event.request.formData()
    let response = await event.fetch(apiUrl + '/search?q=' + fd.get('q'))
    let jsonResponse = await response.json() satisfies QueryResponse
    return jsonResponse
  }
} satisfies Actions;
